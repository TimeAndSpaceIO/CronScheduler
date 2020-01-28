/*
 * Copyright (C) The CronScheduler Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.timeandspace.cronscheduler;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/*
 * This class is an adapted copy of java.util.concurrent.ScheduledThreadPoolExecutor rev. 111:
 * http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/main/
 * java/util/concurrent/ScheduledThreadPoolExecutor.java?revision=1.111&view=markup,
 * written by Doug Lea with assistance from members of JCP JSR-166 Expert Group and released to the
 * public domain, as explained at http://creativecommons.org/publicdomain/zero/1.0/.
 */

/**
 * Changes compared to JSR-166 class:
 * - Specialized {@link DelayedWorkQueue} to deal only with {@link ScheduledFutureTask} objects,
 *   rather than {@link ScheduledFuture}/{@link RunnableFuture}/{@link Delayed}
 * - Replace {@link ScheduledFutureTask}'s getDelay() with {@link
 *   ScheduledFutureTask#getDelayMillis()}, make it to calculate the delay based on the provided
 *   {@link #timeProvider}.
 * - Part of the state of {@link ScheduledFutureTask} is extracted as {@link PeriodicScheduling}.
 * - {@link DelayedWorkQueue} doesn't implement a queue, just a collection which doesn't support
 *   external offering of tasks.
 *
 * Removed:
 * - continueExistingPeriodicTasksAfterShutdown and executeExistingDelayedTasksAfterShutdown fields.
 *   continueExistingPeriodicTasksAfterShutdown is assumed always false,
 *   executeExistingDelayedTasksAfterShutdown is controlled by {@link OneShotTasksShutdownPolicy}
 *   passed to {@link #shutdown}.
 * - canRunInCurrentRunState(task) is inlined as just !isShutdown().
 * - decorateTask() methods.
 * - DelayedWorkQueue.poll() (untimed), take(), offer() methods
 * - Public API schedule(), scheduleAtFixedRate(), scheduleWithFixedDelay() methods. They are
 * replaced with new methods in {@link CronScheduler}.
 */
class ScheduledThreadPoolExecutor extends ThreadPoolExecutor {

    /*
     * This class specializes ThreadPoolExecutor implementation by
     *
     * 1. Using a custom task type ScheduledFutureTask, even for tasks
     *    that don't require scheduling because they are submitted
     *    using ExecutorService rather than ScheduledExecutorService
     *    methods, which are treated as tasks with a delay of zero.
     *
     * 2. Using a custom queue (DelayedWorkQueue), a variant of
     *    unbounded DelayQueue. The lack of capacity constraint and
     *    the fact that corePoolSize and maximumPoolSize are
     *    effectively identical simplifies some execution mechanics
     *    (see delayedExecute) compared to ThreadPoolExecutor.
     *
     * 3. Task decoration methods to allow interception and
     *    instrumentation, which are needed because subclasses cannot
     *    otherwise override submit methods to get this effect. These
     *    don't have any impact on pool control logic though.
     */

    /**
     * True if {@link ScheduledFutureTask#cancel} should remove from queue.
     */
    volatile boolean removeOnCancel;

    /**
     * Sequence number to break scheduling ties, and in turn to
     * guarantee FIFO order among tied entries.
     */
    private static final AtomicLong sequencer = new AtomicLong();

    @NotThreadSafe
    class ScheduledFutureTask<V> extends FutureTask<V> implements ScheduledFuture<V> {

        /** Sequence number to break ties FIFO */
        private final long sequenceNumber;

        /** 0 for periodic tasks. */
        private final long triggerTimeMillis;

        /** null for one-shot tasks. */
        private final PeriodicScheduling<V> periodicScheduling;

        /** Index into delay queue, to support faster cancellation. */
        int heapIndex;

        /** Creates a one-shot action with given trigger time. */
        ScheduledFutureTask(Runnable r, V result, long triggerTimeMillis) {
            super(r, result);
            this.triggerTimeMillis = triggerTimeMillis;
            this.periodicScheduling = null;
            this.sequenceNumber = sequencer.getAndIncrement();
        }

        /** Creates a one-shot action with given trigger time. */
        ScheduledFutureTask(Callable<V> callable, long triggerTimeMillis) {
            super(callable);
            this.triggerTimeMillis = triggerTimeMillis;
            this.periodicScheduling = null;
            this.sequenceNumber = sequencer.getAndIncrement();
        }

        /** Creates a periodic action. */
        ScheduledFutureTask(PeriodicScheduling<V> periodicScheduling) {
            super(Objects.requireNonNull(periodicScheduling));
            this.triggerTimeMillis = 0;
            this.periodicScheduling = periodicScheduling;
            this.sequenceNumber = sequencer.getAndIncrement();
        }

        private long nextScheduledRunTimeMillis() {
            if (periodicScheduling != null) {
                return periodicScheduling.nextScheduledRunTimeMillis();
            } else {
                return triggerTimeMillis;
            }
        }

        long getDelayMillis() {
            return nextScheduledRunTimeMillis() - timeProvider.millis();
        }

        /**
         * @deprecated should not be used within the package, use {@link #getDelayMillis()} instead
         */
        @Deprecated
        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(nextScheduledRunTimeMillis() - timeProvider.millis(), MILLISECONDS);
        }

        @Override
        public int compareTo(@NotNull Delayed other) {
            if (other == this) // compare zero if same object
                return 0;
            if (other instanceof ScheduledFutureTask) {
                ScheduledFutureTask<?> x = (ScheduledFutureTask<?>) other;
                long diff = nextScheduledRunTimeMillis() - x.nextScheduledRunTimeMillis();
                if (diff < 0)
                    return -1;
                else if (diff > 0)
                    return 1;
                else if (sequenceNumber < x.sequenceNumber)
                    return -1;
                else
                    return 1;
            }
            long diff = getDelayMillis() - other.getDelay(MILLISECONDS);
            return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            // The racy read of heapIndex below is benign:
            // if heapIndex < 0, then OOTA guarantees that we have surely
            // been removed; else we recheck under lock in remove()
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled && removeOnCancel && heapIndex >= 0)
                remove(this);
            return cancelled;
        }

        /**
         * Overrides FutureTask version so as to reset/requeue if periodic.
         */
        @Override
        public void run() {
            if (isShutdown())
                cancel(false);
            else if (periodicScheduling == null)
                super.run();
            else if (super.runAndReset()) {
                periodicScheduling.setNextRunTime();
                reExecutePeriodic(this);
            }
        }
    }

    /**
     * Main execution method for delayed or periodic tasks.  If pool
     * is shut down, rejects the task. Otherwise adds task to queue
     * and starts a thread, if necessary, to run it.  (We cannot
     * prestart the thread to run the task because the task (probably)
     * shouldn't be run yet.)  If the pool is shut down while the task
     * is being added, cancel and remove it if required by state and
     * run-after-shutdown parameters.
     *
     * @param task the task
     */
    void delayedExecute(ScheduledFutureTask<?> task) {
        if (isShutdown())
            reject(task);
        else {
            getQueueInternal().addInternal(task);
            if (isShutdown() && remove(task))
                task.cancel(false);
            else
                ensurePrestart();
        }
    }

    /**
     * Requeues a periodic task unless current run state precludes it.
     * Same idea as delayedExecute except drops task rather than rejecting.
     *
     * @param task the task
     */
    void reExecutePeriodic(ScheduledFutureTask<?> task) {
        if (!isShutdown()) {
            getQueueInternal().addInternal(task);
            if (!isShutdown() || !remove(task)) {
                ensurePrestart();
                return;
            }
        }
        task.cancel(false);
    }

    /**
     * Cancels and clears the queue of all tasks that should not be run
     * due to shutdown policy.  Invoked within {@link ThreadPoolExecutor#shutdown}.
     */
    @Override void onShutdown(OneShotTasksShutdownPolicy oneShotTasksShutdownPolicy) {
        DelayedWorkQueue q = getQueueInternal();
        boolean keepDelayed =
                oneShotTasksShutdownPolicy == OneShotTasksShutdownPolicy.EXECUTE_DELAYED;
        // Traverse snapshot to avoid iterator exceptions
        // TODO: implement and use efficient removeIf
        // super.getQueue().removeIf(...);
        for (ScheduledFutureTask<?> t : q.toArray()) {
            if ((t.periodicScheduling != null ||
                    (!keepDelayed && t.getDelayMillis() > 0)) ||
                    t.isCancelled()) { // also remove if already cancelled
                if (q.remove(t))
                    t.cancel(false);
            }
        }
        tryTerminate();
    }

    /**
     * Creates a new {@code ScheduledThreadPoolExecutor} with the
     * given initial parameters.
     *
     * @param threadFactory the factory to use when the executor
     *        creates a new thread
     * @throws NullPointerException if {@code threadFactory} is null
     */
    public ScheduledThreadPoolExecutor(Clock timeProvider, Duration syncPeriod,
            ThreadFactory threadFactory, Consumer<String> backwardTimeShiftLogger) {
        super(timeProvider, syncPeriod, new DelayedWorkQueue(), threadFactory,
                backwardTimeShiftLogger);
    }

    /**
     * Sets the policy on whether cancelled tasks should be immediately
     * removed from the work queue at time of cancellation.  This value is
     * by default {@code false}.
     *
     * @param value if {@code true}, remove on cancellation, else don't
     * @see #getRemoveOnCancelPolicy
     */
    void setRemoveOnCancelPolicy(boolean value) {
        removeOnCancel = value;
    }

    /**
     * Gets the policy on whether cancelled tasks should be immediately
     * removed from the work queue at time of cancellation.  This value is
     * by default {@code false}.
     *
     * @return {@code true} if cancelled tasks are immediately removed
     *         from the queue
     * @see #setRemoveOnCancelPolicy
     */
    boolean getRemoveOnCancelPolicy() {
        return removeOnCancel;
    }

    @Override
    public void shutdown(OneShotTasksShutdownPolicy oneShotTasksShutdownPolicy) {
        super.shutdown(oneShotTasksShutdownPolicy);
    }

    /**
     * Attempts to stop all actively executing tasks, halts the
     * processing of waiting tasks, and returns a list of the tasks
     * that were awaiting execution. These tasks are drained (removed)
     * from the task queue upon return from this method.
     *
     * <p>This method does not wait for actively executing tasks to
     * terminate.  Use {@link #awaitTermination awaitTermination} to
     * do that.
     *
     * <p>There are no guarantees beyond best-effort attempts to stop
     * processing actively executing tasks.  This implementation
     * interrupts tasks via {@link Thread#interrupt}; any task that
     * fails to respond to interrupts may never terminate.
     *
     * @return list of tasks that were awaiting execution (one-shot or periodic). The elements are
     *         identical to the futures returned from {@code schedule} methods.
     * @throws SecurityException if a security manager exists and
     *         shutting down this ExecutorService may manipulate
     *         threads that the caller is not permitted to modify
     *         because it does not hold {@link
     *         java.lang.RuntimePermission}{@code ("modifyThread")},
     *         or the security manager's {@code checkAccess} method
     *         denies access.
     */
    public List<Future<?>> shutdownNow() {
        return super.shutdownNow();
    }

    /**
     * Returns the task collection used by this CronScheduler.  Access to the
     * task collection is intended primarily for debugging and monitoring.
     * This collection may be in active use.  Retrieving the task collection
     * does not prevent queued tasks from executing.
     *
     * <p>Each element of this collection is a {@link Future} returned from one of the {@code
     * schedule} methods.
     *
     * <p>Iteration over this collection is <em>not</em> guaranteed to traverse
     * tasks in the order in which they will execute.
     *
     * <p>Attempts to add elements to this collection will throw {@link
     * UnsupportedOperationException}.
     *
     * @return the task collection
     */
    public Collection<? extends Future<?>> getTasks() {
        return getQueueInternal();
    }

    /**
     * Specialized delay queue. To mesh with TPE declarations, this
     * class must be declared as a BlockingQueue<Runnable> even though
     * it can only hold ScheduledFutureTasks.
     */
    static class DelayedWorkQueue extends AbstractCollection<ScheduledFutureTask<?>> {

        /*
         * A DelayedWorkQueue is based on a heap-based data structure
         * like those in DelayQueue and PriorityQueue, except that
         * every ScheduledFutureTask also records its index into the
         * heap array. This eliminates the need to find a task upon
         * cancellation, greatly speeding up removal (down from O(n)
         * to O(log n)), and reducing garbage retention that would
         * otherwise occur by waiting for the element to rise to top
         * before clearing. But because the queue may also hold
         * ScheduledFutureTasks that are not ScheduledFutureTasks,
         * we are not guaranteed to have such indices available, in
         * which case we fall back to linear search. (We expect that
         * most tasks will not be decorated, and that the faster cases
         * will be much more common.)
         *
         * All heap operations must record index changes -- mainly
         * within siftUp and siftDown. Upon removal, a task's
         * heapIndex is set to -1. Note that ScheduledFutureTasks can
         * appear at most once in the queue (this need not be true for
         * other kinds of tasks or work queues), so are uniquely
         * identified by heapIndex.
         */

        private static final int INITIAL_CAPACITY = 16;
        @GuardedBy("lock")
        private ScheduledFutureTask<?>[] queue =
                new ScheduledFutureTask<?>[INITIAL_CAPACITY];
        private final ReentrantLock lock = new ReentrantLock();
        @GuardedBy("lock")
        private int size;

        /**
         * Thread designated to wait for the task at the head of the
         * queue.  This variant of the Leader-Follower pattern
         * (http://www.cs.wustl.edu/~schmidt/POSA/POSA2/) serves to
         * minimize unnecessary timed waiting.  When a thread becomes
         * the leader, it waits only for the next delay to elapse, but
         * other threads await indefinitely.  The leader thread must
         * signal some other thread before returning from take() or
         * poll(...), unless some other thread becomes leader in the
         * interim.  Whenever the head of the queue is replaced with a
         * task with an earlier expiration time, the leader field is
         * invalidated by being reset to null, and some waiting
         * thread, but not necessarily the current leader, is
         * signalled.  So waiting threads must be prepared to acquire
         * and lose leadership while waiting.
         */
        @GuardedBy("lock")
        private Thread leader;

        /**
         * Condition signalled when a newer task becomes available at the
         * head of the queue or a new thread may need to become leader.
         */
        @GuardedBy("lock")
        private final Condition available = lock.newCondition();

        /**
         * Sets f's heapIndex if it is a ScheduledFutureTask.
         */
        private static void setIndex(ScheduledFutureTask<?> f, int idx) {
            f.heapIndex = idx;
        }

        /** Sifts element added at bottom up to its heap-ordered spot. */
        @GuardedBy("lock")
        private void siftUp(int k, ScheduledFutureTask<?> key) {
            while (k > 0) {
                int parent = (k - 1) >>> 1;
                ScheduledFutureTask<?> e = queue[parent];
                if (key.compareTo(e) >= 0)
                    break;
                queue[k] = e;
                setIndex(e, k);
                k = parent;
            }
            queue[k] = key;
            setIndex(key, k);
        }

        /** Sifts element added at top down to its heap-ordered spot. */
        @GuardedBy("lock")
        private void siftDown(int k, ScheduledFutureTask<?> key) {
            int half = size >>> 1;
            while (k < half) {
                int child = (k << 1) + 1;
                ScheduledFutureTask<?> c = queue[child];
                int right = child + 1;
                if (right < size && c.compareTo(queue[right]) > 0)
                    c = queue[child = right];
                if (key.compareTo(c) <= 0)
                    break;
                queue[k] = c;
                setIndex(c, k);
                k = child;
            }
            queue[k] = key;
            setIndex(key, k);
        }

        /** Resizes the heap array. */
        @GuardedBy("lock")
        private void grow() {
            int oldCapacity = queue.length;
            int newCapacity = oldCapacity + (oldCapacity >> 1); // grow 50%
            if (newCapacity < 0) // overflow
                newCapacity = Integer.MAX_VALUE;
            queue = Arrays.copyOf(queue, newCapacity);
        }

        /** Finds index of given object, or -1 if absent. */
        @GuardedBy("lock")
        private int indexOf(Object x) {
            if (x != null) {
                if (x instanceof ScheduledFutureTask) {
                    int i = ((ScheduledFutureTask<?>) x).heapIndex;
                    // Sanity check; x could conceivably be a
                    // ScheduledFutureTask from some other pool.
                    if (i >= 0 && i < size && queue[i] == x)
                        return i;
                } else {
                    for (int i = 0; i < size; i++)
                        if (x.equals(queue[i]))
                            return i;
                }
            }
            return -1;
        }

        @Override
        public boolean contains(Object x) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return indexOf(x) != -1;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean remove(Object x) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                int i = indexOf(x);
                if (i < 0)
                    return false;

                setIndex(queue[i], -1);
                int s = --size;
                ScheduledFutureTask<?> replacement = queue[s];
                queue[s] = null;
                if (s != i) {
                    siftDown(i, replacement);
                    if (queue[i] == replacement)
                        siftUp(i, replacement);
                }
                return true;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public int size() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return size;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        /**
         * Doesn't override {@link #add} so that when a queue is returned from {@link #getTasks()}
         * tasks couldn't be added externally.
         */
        void addInternal(ScheduledFutureTask<?> e) {
            if (e == null)
                throw new NullPointerException();
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                int i = size;
                if (i >= queue.length)
                    grow();
                size = i + 1;
                if (i == 0) {
                    queue[0] = e;
                    setIndex(e, 0);
                } else {
                    siftUp(i, e);
                }
                if (queue[0] == e) {
                    leader = null;
                    available.signal();
                }
            } finally {
                lock.unlock();
            }
        }

        void rebuild(long newTimeMillis) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                for (int i = 0; i < size; i++) {
                    ScheduledFutureTask<?> task = queue[i];
                    if (task.periodicScheduling != null) {
                        if (task.periodicScheduling.rewind(newTimeMillis)) {
                            siftUp(i, task);
                        }
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Performs common bookkeeping for poll and take: Replaces
         * first element with last and sifts it down.  Call only when
         * holding lock.
         * @param f the task to remove and return
         */
        private ScheduledFutureTask<?> finishPoll(ScheduledFutureTask<?> f) {
            int s = --size;
            ScheduledFutureTask<?> x = queue[s];
            queue[s] = null;
            if (s != 0)
                siftDown(0, x);
            setIndex(f, -1);
            return f;
        }

        public ScheduledFutureTask<?> poll(long timeout, TimeUnit unit)
                throws InterruptedException {
            long nanos = unit.toNanos(timeout);
            final ReentrantLock lock = this.lock;
            lock.lockInterruptibly();
            try {
                for (;;) {
                    ScheduledFutureTask<?> first = queue[0];
                    if (first == null) {
                        if (nanos <= 0L)
                            return null;
                        else
                            nanos = available.awaitNanos(nanos);
                    } else {
                        long delay = MILLISECONDS.toNanos(first.getDelayMillis());
                        if (delay <= 0L)
                            return finishPoll(first);
                        if (nanos <= 0L)
                            return null;
                        //noinspection UnusedAssignment: don't retain ref while waiting
                        first = null;
                        if (nanos < delay || leader != null)
                            nanos = available.awaitNanos(nanos);
                        else {
                            Thread thisThread = Thread.currentThread();
                            leader = thisThread;
                            try {
                                long timeLeft = available.awaitNanos(delay);
                                nanos -= delay - timeLeft;
                            } finally {
                                if (leader == thisThread)
                                    leader = null;
                            }
                        }
                    }
                }
            } finally {
                if (leader == null && queue[0] != null)
                    available.signal();
                lock.unlock();
            }
        }

        public void clear() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                for (int i = 0; i < size; i++) {
                    ScheduledFutureTask<?> t = queue[i];
                    if (t != null) {
                        queue[i] = null;
                        setIndex(t, -1);
                    }
                }
                size = 0;
            } finally {
                lock.unlock();

            }
        }

        void drainTo(Collection<? super ScheduledFutureTask<?>> c) {
            Objects.requireNonNull(c);
            if (c == this)
                throw new IllegalArgumentException();
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                for (ScheduledFutureTask<?> first;
                     (first = queue[0]) != null && first.getDelayMillis() <= 0;) {
                    c.add(first);   // In this order, in case add() throws.
                    finishPoll(first);
                }
            } finally {
                lock.unlock();
            }
        }

        @NotNull
        @Override
        public ScheduledFutureTask<?>[] toArray() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return Arrays.copyOf(queue, size, ScheduledFutureTask[].class);
            } finally {
                lock.unlock();
            }
        }

        @NotNull
        @Override
        @SuppressWarnings("unchecked")
        public <T> T[] toArray(T[] a) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                if (a.length < size)
                    return (T[]) Arrays.copyOf(queue, size, a.getClass());
                //noinspection SuspiciousSystemArraycopy
                System.arraycopy(queue, 0, a, 0, size);
                if (a.length > size)
                    a[size] = null;
                return a;
            } finally {
                lock.unlock();
            }
        }

        @NotNull
        @Override
        public Iterator<ScheduledFutureTask<?>> iterator() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return new Itr(Arrays.copyOf(queue, size));
            } finally {
                lock.unlock();
            }
        }

        /**
         * Snapshot iterator that works off copy of underlying q array.
         */
        private class Itr implements Iterator<ScheduledFutureTask<?>> {
            final ScheduledFutureTask<?>[] array;
            int cursor;        // index of next element to return; initially 0
            int lastRet = -1;  // index of last element returned; -1 if no such

            Itr(ScheduledFutureTask<?>[] array) {
                this.array = array;
            }

            public boolean hasNext() {
                return cursor < array.length;
            }

            public ScheduledFutureTask<?> next() {
                if (cursor >= array.length)
                    throw new NoSuchElementException();
                return array[lastRet = cursor++];
            }

            public void remove() {
                if (lastRet < 0)
                    throw new IllegalStateException();
                DelayedWorkQueue.this.remove(array[lastRet]);
                lastRet = -1;
            }
        }
    }
}

