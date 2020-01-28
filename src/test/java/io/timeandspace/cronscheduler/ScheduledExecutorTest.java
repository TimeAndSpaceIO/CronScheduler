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

import junit.framework.Test;
import junit.framework.TestSuite;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */
public class ScheduledExecutorTest extends JSR166TestCase {

    public static final Duration ONE_SEC = Duration.ofSeconds(1);

    public static void main(String[] args) {
        main(suite(), args);
    }
    public static Test suite() {
        return new TestSuite(ScheduledExecutorTest.class);
    }

    private static ScheduledExecutorService createCronScheduler() {
        return CronScheduler.create(ONE_SEC).asScheduledExecutorService();
    }

    /**
     * execute successfully executes a runnable
     */
    public void testExecute() throws InterruptedException {
        final ScheduledExecutorService p = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(p)) {
            final CountDownLatch done = new CountDownLatch(1);
            final Runnable task = new CheckedRunnable() {
                public void realRun() { done.countDown(); }};
            p.execute(task);
            await(done);
        }
    }

    /**
     * delayed schedule of callable successfully executes after delay
     */
    public void testSchedule1() throws Exception {
        final ScheduledExecutorService p = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(p)) {
            final long startTime = System.currentTimeMillis();
            final CountDownLatch done = new CountDownLatch(1);
            Callable task = new CheckedCallable<Boolean>() {
                public Boolean realCall() {
                    done.countDown();
                    assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
                    return Boolean.TRUE;
                }};
            Future f = p.schedule(task, timeoutMillis(), MILLISECONDS);
            assertSame(Boolean.TRUE, f.get());
            assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
            assertEquals(0L, done.getCount());
        }
    }

    /**
     * delayed schedule of runnable successfully executes after delay
     */
    public void testSchedule3() throws Exception {
        final ScheduledExecutorService p = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(p)) {
            final long startTime = System.currentTimeMillis();
            final CountDownLatch done = new CountDownLatch(1);
            Runnable task = new CheckedRunnable() {
                public void realRun() {
                    done.countDown();
                    assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
                }};
            Future f = p.schedule(task, timeoutMillis(), MILLISECONDS);
            await(done);
            assertNull(f.get(LONG_DELAY_MS, MILLISECONDS));
            assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
        }
    }

    /**
     * scheduleAtFixedRate executes runnable after given initial delay
     */
    public void testSchedule4() throws Exception {
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        try (PoolCleaner cleaner = cleaner(p)) {
            final long startTime = System.currentTimeMillis();
            final CountDownLatch done = new CountDownLatch(1);
            Runnable task = new CheckedRunnable() {
                public void realRun() {
                    done.countDown();
                    assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
                }};
            ScheduledFuture f =
                    p.asScheduledExecutorService().scheduleAtFixedRate(task, timeoutMillis(),
                            LONG_DELAY_MS, MILLISECONDS);
            await(done);
            assertTrue(millisElapsedSince(startTime) >= timeoutMillis());
            f.cancel(true);
        }
    }

    static class RunnableCounter implements Runnable {
        AtomicInteger count = new AtomicInteger(0);
        public void run() { count.getAndIncrement(); }
    }

    /**
     * scheduleAtFixedRate executes series of tasks at given rate.
     * Eventually, it must hold that:
     *   cycles - 1 <= elapsedMillis/delay < cycles
     */
    public void testFixedRateSequence() throws InterruptedException {
        final ScheduledExecutorService p = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(p)) {
            for (int delay = 1; delay <= LONG_DELAY_MS; delay *= 3) {
                final long startTime = System.currentTimeMillis();
                final int cycles = 8;
                final CountDownLatch done = new CountDownLatch(cycles);
                final Runnable task = new CheckedRunnable() {
                    public void realRun() { done.countDown(); }};
                final ScheduledFuture periodicTask =
                        p.scheduleAtFixedRate(task, 0, delay, MILLISECONDS);
                final int totalDelayMillis = (cycles - 1) * delay;
                await(done, totalDelayMillis + LONG_DELAY_MS);
                periodicTask.cancel(true);
                final long elapsedMillis = millisElapsedSince(startTime);
                assertThat(elapsedMillis).isGreaterThanOrEqualTo(totalDelayMillis);
                if (elapsedMillis <= cycles * delay)
                    return;
                // else retry with longer delay
            }
            fail("unexpected execution rate");
        }
    }

    /**
     * Submitting null tasks throws NullPointerException
     */
    public void testNullTaskSubmission() {
        final ScheduledExecutorService p = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(p)) {
            assertNullTaskSubmissionThrowsNullPointerException(p);
        }
    }

    /**
     * Submitted tasks are rejected when shutdown
     */
    public void testSubmittedTasksRejectedWhenShutdown() throws InterruptedException {
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        final ScheduledExecutorService ses = p.asScheduledExecutorService();
        final ThreadLocalRandom rnd = ThreadLocalRandom.current();
        final CountDownLatch threadsStarted = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(1);
        final Runnable r = () -> {
            threadsStarted.countDown();
            for (;;) {
                try {
                    done.await();
                    return;
                } catch (InterruptedException shutdownNowDeliberatelyIgnored) {}
            }};
        final Callable<Boolean> c = () -> {
            threadsStarted.countDown();
            for (;;) {
                try {
                    done.await();
                    return Boolean.TRUE;
                } catch (InterruptedException shutdownNowDeliberatelyIgnored) {}
            }};

        try (PoolCleaner cleaner = cleaner(ses, done)) {
            for (int i = 1; i--> 0; ) {
                switch (rnd.nextInt(4)) {
                    case 0: ses.execute(r); break;
                    case 1: assertFalse(ses.submit(r).isDone()); break;
                    case 2: assertFalse(ses.submit(r, Boolean.TRUE).isDone()); break;
                    case 3: assertFalse(ses.submit(c).isDone()); break;
                }
            }

            // ScheduledThreadPoolExecutor has an unbounded queue, so never saturated.
            await(threadsStarted);

            if (rnd.nextBoolean())
                p.shutdownNow();
            else
                ses.shutdown();
            // Pool is shutdown, but not yet terminated
            assertTaskSubmissionsAreRejected(p);
            assertFalse(p.isTerminated());

            done.countDown();   // release blocking tasks
            assertTrue(p.awaitTermination(LONG_DELAY_MS, MILLISECONDS));

            assertTaskSubmissionsAreRejected(p);
        }
        assertEquals(1, p.getCompletedTaskCount());
    }

    /**
     * getActiveCount increases but doesn't overestimate, when a
     * thread becomes active
     */
    public void testGetActiveCount() throws InterruptedException {
        final CountDownLatch done = new CountDownLatch(1);
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        try (PoolCleaner cleaner = cleaner(p.asScheduledExecutorService(), done)) {
            final CountDownLatch threadStarted = new CountDownLatch(1);
            assertFalse(p.isThreadActive());
            p.asScheduledExecutorService().execute(new CheckedRunnable() {
                public void realRun() throws InterruptedException {
                    threadStarted.countDown();
                    assertTrue(p.isThreadActive());
                    await(done);
                }});
            await(threadStarted);
            assertTrue(p.isThreadActive());
        }
    }

    /**
     * getCompletedTaskCount increases, but doesn't overestimate,
     * when tasks complete
     */
    public void testGetCompletedTaskCount() throws InterruptedException {
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        try (PoolCleaner cleaner = cleaner(p.asScheduledExecutorService())) {
            final CountDownLatch threadStarted = new CountDownLatch(1);
            final CountDownLatch threadProceed = new CountDownLatch(1);
            final CountDownLatch threadDone = new CountDownLatch(1);
            assertEquals(0, p.getCompletedTaskCount());
            p.asScheduledExecutorService().execute(new CheckedRunnable() {
                public void realRun() throws InterruptedException {
                    threadStarted.countDown();
                    assertEquals(0, p.getCompletedTaskCount());
                    await(threadProceed);
                    threadDone.countDown();
                }});
            await(threadStarted);
            assertEquals(0, p.getCompletedTaskCount());
            threadProceed.countDown();
            await(threadDone);
            long startTime = System.currentTimeMillis();
            while (p.getCompletedTaskCount() != 1) {
                if (millisElapsedSince(startTime) > LONG_DELAY_MS)
                    fail("timed out");
                Thread.yield();
            }
        }
    }

    /**
     * getPoolSize increases, but doesn't overestimate, when threads
     * become active
     */
    public void testGetPoolSize() throws InterruptedException {
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        final CountDownLatch threadStarted = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(1);
        try (PoolCleaner cleaner = cleaner(p.asScheduledExecutorService(), done)) {
            assertFalse(p.isThreadRunning());
            p.asScheduledExecutorService().execute(new CheckedRunnable() {
                public void realRun() throws InterruptedException {
                    threadStarted.countDown();
                    assertTrue(p.isThreadRunning());
                    await(done);
                }});
            await(threadStarted);
            assertTrue(p.isThreadRunning());
        }
    }

    /**
     * getTaskCount increases, but doesn't overestimate, when tasks
     * submitted
     */
    public void testGetTaskCount() throws InterruptedException {
        final int TASKS = 3;
        final CountDownLatch done = new CountDownLatch(1);
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        final ScheduledExecutorService ses = p.asScheduledExecutorService();
        try (PoolCleaner cleaner = cleaner(ses, done)) {
            final CountDownLatch threadStarted = new CountDownLatch(1);
            assertEquals(0, p.getTaskCount());
            assertEquals(0, p.getCompletedTaskCount());
            ses.execute(new CheckedRunnable() {
                public void realRun() throws InterruptedException {
                    threadStarted.countDown();
                    await(done);
                }});
            await(threadStarted);
            assertEquals(1, p.getTaskCount());
            assertEquals(0, p.getCompletedTaskCount());
            for (int i = 0; i < TASKS; i++) {
                assertEquals(1 + i, p.getTaskCount());
                ses.execute(new CheckedRunnable() {
                    public void realRun() throws InterruptedException {
                        threadStarted.countDown();
                        assertEquals(1 + TASKS, p.getTaskCount());
                        await(done);
                    }});
            }
            assertEquals(1 + TASKS, p.getTaskCount());
            assertEquals(0, p.getCompletedTaskCount());
        }
        assertEquals(1 + TASKS, p.getTaskCount());
        assertEquals(1 + TASKS, p.getCompletedTaskCount());
    }

    /**
     * isShutdown is false before shutdown, true after
     */
    public void testIsShutdown() {
        final ScheduledExecutorService p = createCronScheduler();
        assertFalse(p.isShutdown());
        try (PoolCleaner cleaner = cleaner(p)) {
            try {
                p.shutdown();
                assertTrue(p.isShutdown());
            } catch (SecurityException ok) {}
        }
    }

    /**
     * isTerminated is false before termination, true after
     */
    public void testIsTerminated() throws InterruptedException {
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        try (PoolCleaner cleaner = cleaner(p)) {
            final CountDownLatch threadStarted = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(1);
            assertFalse(p.isTerminated());
            p.asScheduledExecutorService().execute(new CheckedRunnable() {
                public void realRun() throws InterruptedException {
                    assertFalse(p.isTerminated());
                    threadStarted.countDown();
                    await(done);
                }});
            await(threadStarted);
            assertFalse(p.isTerminating());
            done.countDown();
            if (shutdown(p)) return;
            assertTrue(p.awaitTermination(LONG_DELAY_MS, MILLISECONDS));
            assertTrue(p.isTerminated());
        }
    }

    private static boolean shutdown(CronScheduler p) {
        try {
            p.shutdown(OneShotTasksShutdownPolicy.EXECUTE_DELAYED);
        } catch (SecurityException ok) {
            return true;
        }
        return false;
    }

    /**
     * isTerminating is not true when running or when terminated
     */
    public void testIsTerminating() throws InterruptedException {
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        final CountDownLatch threadStarted = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(1);
        try (PoolCleaner cleaner = cleaner(p)) {
            assertFalse(p.isTerminating());
            p.asScheduledExecutorService().execute(new CheckedRunnable() {
                public void realRun() throws InterruptedException {
                    assertFalse(p.isTerminating());
                    threadStarted.countDown();
                    await(done);
                }});
            await(threadStarted);
            assertFalse(p.isTerminating());
            done.countDown();
            shutdown(p);
            assertTrue(p.awaitTermination(LONG_DELAY_MS, MILLISECONDS));
            assertTrue(p.isTerminated());
            assertFalse(p.isTerminating());
        }
    }

    /**
     * getQueue returns the work queue, which contains queued tasks
     */
    public void testGetQueue() throws InterruptedException {
        final CountDownLatch done = new CountDownLatch(1);
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        try (PoolCleaner cleaner = cleaner(p.asScheduledExecutorService(), done)) {
            final CountDownLatch threadStarted = new CountDownLatch(1);
            ScheduledFuture[] tasks = new ScheduledFuture[5];
            for (int i = 0; i < tasks.length; i++) {
                Runnable r = new CheckedRunnable() {
                    public void realRun() throws InterruptedException {
                        threadStarted.countDown();
                        await(done);
                    }};
                tasks[i] = p.asScheduledExecutorService().schedule(r, 1, MILLISECONDS);
            }
            await(threadStarted);
            Collection<? extends Future<?>> q = p.getTasks();
            assertTrue(q.contains(tasks[tasks.length - 1]));
            assertFalse(q.contains(tasks[0]));
        }
    }

    /**
     * remove(task) removes queued task, and fails to remove active task
     */
    public void testRemove() throws InterruptedException {
        final CountDownLatch done = new CountDownLatch(1);
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        try (PoolCleaner cleaner = cleaner(p.asScheduledExecutorService(), done)) {
            ScheduledThreadPoolExecutor.ScheduledFutureTask<?>[] tasks =
                    new ScheduledThreadPoolExecutor.ScheduledFutureTask<?>[5];
            final CountDownLatch threadStarted = new CountDownLatch(1);
            for (int i = 0; i < tasks.length; i++) {
                Runnable r = new CheckedRunnable() {
                    public void realRun() throws InterruptedException {
                        threadStarted.countDown();
                        await(done);
                    }};
                tasks[i] = (ScheduledThreadPoolExecutor.ScheduledFutureTask<?>)
                        p.asScheduledExecutorService().schedule(r, 1, MILLISECONDS);
            }
            await(threadStarted);
            Collection<? extends Future<?>> q = p.getTasks();
            assertFalse(p.remove(tasks[0]));
            assertTrue(q.contains(tasks[4]));
            assertTrue(q.contains(tasks[3]));
            assertTrue(p.remove(tasks[4]));
            assertFalse(p.remove(tasks[4]));
            assertFalse(q.contains(tasks[4]));
            assertTrue(q.contains(tasks[3]));
            assertTrue(p.remove(tasks[3]));
            assertFalse(q.contains(tasks[3]));
        }
    }

    /**
     * purge eventually removes cancelled tasks from the queue
     */
    public void testPurge() throws InterruptedException {
        final ScheduledFuture[] tasks = new ScheduledFuture[5];
        final Runnable releaser = new Runnable() { public void run() {
            for (ScheduledFuture task : tasks)
                if (task != null) task.cancel(true); }};
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        try (PoolCleaner cleaner = cleaner(p.asScheduledExecutorService(), releaser)) {
            for (int i = 0; i < tasks.length; i++)
                tasks[i] = p.asScheduledExecutorService()
                        .schedule(possiblyInterruptedRunnable(SMALL_DELAY_MS),
                                LONG_DELAY_MS, MILLISECONDS);
            int max = tasks.length;
            if (tasks[4].cancel(true)) --max;
            if (tasks[3].cancel(true)) --max;
            // There must eventually be an interference-free point at
            // which purge will not fail. (At worst, when queue is empty.)
            long startTime = System.currentTimeMillis();
            do {
                p.purge();
                long count = p.getTaskCount();
                if (count == max)
                    return;
            } while (millisElapsedSince(startTime) < LONG_DELAY_MS);
            fail("Purge failed to remove cancelled tasks");
        }
    }

    /**
     * shutdownNow returns a list containing tasks that were not run,
     * and those tasks are drained from the queue
     */
    public void testShutdownNow() throws InterruptedException {
        final int poolSize = 1;
        final int count = 5;
        final AtomicInteger ran = new AtomicInteger(0);
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        final CountDownLatch threadsStarted = new CountDownLatch(poolSize);
        Runnable waiter = new CheckedRunnable() { public void realRun() {
            threadsStarted.countDown();
            try {
                MILLISECONDS.sleep(LONGER_DELAY_MS);
            } catch (InterruptedException success) {}
            ran.getAndIncrement();
        }};
        for (int i = 0; i < count; i++)
            p.asScheduledExecutorService().execute(waiter);
        await(threadsStarted);
        assertTrue(p.isThreadActive());
        assertEquals(0, p.getCompletedTaskCount());
        final List<Future<?>> queuedTasks;
        try {
            queuedTasks = p.shutdownNow();
        } catch (SecurityException ok) {
            return; // Allowed in case test doesn't have privs
        }
        assertTrue(p.isShutdown());
        assertTrue(p.getTasks().isEmpty());
        assertEquals(count - poolSize, queuedTasks.size());
        assertTrue(p.awaitTermination(LONG_DELAY_MS, MILLISECONDS));
        assertTrue(p.isTerminated());
        assertEquals(poolSize, ran.get());
        assertEquals(poolSize, p.getCompletedTaskCount());
    }

    /**
     * shutdownNow returns a list containing tasks that were not run,
     * and those tasks are drained from the queue
     */
    public void testShutdownNow_delayedTasks() throws InterruptedException {
        final CronScheduler p = CronScheduler.create(ONE_SEC);
        final ScheduledExecutorService ses = p.asScheduledExecutorService();
        List<ScheduledFuture> tasks = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Runnable r = new NoOpRunnable();
            tasks.add(ses.schedule(r, 9, SECONDS));
            tasks.add(ses.scheduleAtFixedRate(r, 9, 9, SECONDS));
        }
        if (testImplementationDetails)
            assertEquals(new HashSet(tasks), new HashSet(p.getTasks()));
        final List<Future<?>> queuedTasks;
        try {
            queuedTasks = p.shutdownNow();
        } catch (SecurityException ok) {
            return; // Allowed in case test doesn't have privs
        }
        assertTrue(p.isShutdown());
        assertTrue(p.getTasks().isEmpty());
        if (testImplementationDetails)
            assertEquals(new HashSet(tasks), new HashSet(queuedTasks));
        assertEquals(tasks.size(), queuedTasks.size());
        for (ScheduledFuture task : tasks) {
            assertFalse(task.isDone());
            assertFalse(task.isCancelled());
        }
        assertTrue(p.awaitTermination(LONG_DELAY_MS, MILLISECONDS));
        assertTrue(p.isTerminated());
    }

    /**
     * completed submit of callable returns result
     */
    public void testSubmitCallable() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            Future<String> future = e.submit(new StringTask());
            String result = future.get();
            assertSame(TEST_STRING, result);
        }
    }

    /**
     * completed submit of runnable returns successfully
     */
    public void testSubmitRunnable() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            Future<?> future = e.submit(new NoOpRunnable());
            future.get();
            assertTrue(future.isDone());
        }
    }

    /**
     * completed submit of (runnable, result) returns result
     */
    public void testSubmitRunnable2() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            Future<String> future = e.submit(new NoOpRunnable(), TEST_STRING);
            String result = future.get();
            assertSame(TEST_STRING, result);
        }
    }

    /**
     * invokeAny(null) throws NullPointerException
     */
    public void testInvokeAny1() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            try {
                e.invokeAny(null);
                shouldThrow();
            } catch (NullPointerException success) {}
        }
    }

    /**
     * invokeAny(empty collection) throws IllegalArgumentException
     */
    public void testInvokeAny2() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            try {
                e.invokeAny(new ArrayList<Callable<String>>());
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * invokeAny(c) throws NullPointerException if c has null elements
     */
    public void testInvokeAny3() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<>();
            l.add(latchAwaitingStringTask(latch));
            l.add(null);
            try {
                e.invokeAny(l);
                shouldThrow();
            } catch (NullPointerException success) {}
            latch.countDown();
        }
    }

    /**
     * invokeAny(c) throws ExecutionException if no task completes
     */
    public void testInvokeAny4() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<>();
            l.add(new NPETask());
            try {
                e.invokeAny(l);
                shouldThrow();
            } catch (ExecutionException success) {
                assertTrue(success.getCause() instanceof NullPointerException);
            }
        }
    }

    /**
     * invokeAny(c) returns result of some task
     */
    public void testInvokeAny5() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<>();
            l.add(new StringTask());
            l.add(new StringTask());
            String result = e.invokeAny(l);
            assertSame(TEST_STRING, result);
        }
    }

    /**
     * invokeAll(null) throws NPE
     */
    public void testInvokeAll1() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            try {
                e.invokeAll(null);
                shouldThrow();
            } catch (NullPointerException success) {}
        }
    }

    /**
     * invokeAll(empty collection) returns empty list
     */
    public void testInvokeAll2() throws Exception {
        final ExecutorService e = createCronScheduler();
        final Collection<Callable<String>> emptyCollection
                = Collections.emptyList();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Future<String>> r = e.invokeAll(emptyCollection);
            assertTrue(r.isEmpty());
        }
    }

    /**
     * invokeAll(c) throws NPE if c has null elements
     */
    public void testInvokeAll3() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<>();
            l.add(new StringTask());
            l.add(null);
            try {
                e.invokeAll(l);
                shouldThrow();
            } catch (NullPointerException success) {}
        }
    }

    /**
     * get of invokeAll(c) throws exception on failed task
     */
    public void testInvokeAll4() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<>();
            l.add(new NPETask());
            List<Future<String>> futures = e.invokeAll(l);
            assertEquals(1, futures.size());
            try {
                futures.get(0).get();
                shouldThrow();
            } catch (ExecutionException success) {
                assertTrue(success.getCause() instanceof NullPointerException);
            }
        }
    }

    /**
     * invokeAll(c) returns results of all completed tasks
     */
    public void testInvokeAll5() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<>();
            l.add(new StringTask());
            l.add(new StringTask());
            List<Future<String>> futures = e.invokeAll(l);
            assertEquals(2, futures.size());
            for (Future<String> future : futures)
                assertSame(TEST_STRING, future.get());
        }
    }

    /**
     * timed invokeAny(null) throws NPE
     */
    public void testTimedInvokeAny1() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            try {
                e.invokeAny(null, randomTimeout(), randomTimeUnit());
                shouldThrow();
            } catch (NullPointerException success) {}
        }
    }

    /**
     * timed invokeAny(,,null) throws NullPointerException
     */
    public void testTimedInvokeAnyNullTimeUnit() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<>();
            l.add(new StringTask());
            try {
                e.invokeAny(l, randomTimeout(), null);
                shouldThrow();
            } catch (NullPointerException success) {}
        }
    }

    /**
     * timed invokeAny(empty collection) throws IllegalArgumentException
     */
    public void testTimedInvokeAny2() throws Exception {
        final ExecutorService e = createCronScheduler();
        final Collection<Callable<String>> emptyCollection
                = Collections.emptyList();
        try (PoolCleaner cleaner = cleaner(e)) {
            try {
                e.invokeAny(emptyCollection, randomTimeout(), randomTimeUnit());
                shouldThrow();
            } catch (IllegalArgumentException success) {}
        }
    }

    /**
     * timed invokeAny(c) throws NPE if c has null elements
     */
    public void testTimedInvokeAny3() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<>();
            l.add(latchAwaitingStringTask(latch));
            l.add(null);
            try {
                e.invokeAny(l, randomTimeout(), randomTimeUnit());
                shouldThrow();
            } catch (NullPointerException success) {}
            latch.countDown();
        }
    }

    /**
     * timed invokeAny(c) throws ExecutionException if no task completes
     */
    public void testTimedInvokeAny4() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            long startTime = System.currentTimeMillis();
            List<Callable<String>> l = new ArrayList<>();
            l.add(new NPETask());
            try {
                e.invokeAny(l, LONG_DELAY_MS, MILLISECONDS);
                shouldThrow();
            } catch (ExecutionException success) {
                assertTrue(success.getCause() instanceof NullPointerException);
            }
            assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS);
        }
    }

    /**
     * timed invokeAny(c) returns result of some task
     */
    public void testTimedInvokeAny5() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            long startTime = System.currentTimeMillis();
            List<Callable<String>> l = new ArrayList<>();
            l.add(new StringTask());
            l.add(new StringTask());
            String result = e.invokeAny(l, LONG_DELAY_MS, MILLISECONDS);
            assertSame(TEST_STRING, result);
            assertTrue(millisElapsedSince(startTime) < LONG_DELAY_MS);
        }
    }

    /**
     * timed invokeAll(null) throws NPE
     */
    public void testTimedInvokeAll1() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            try {
                e.invokeAll(null, randomTimeout(), randomTimeUnit());
                shouldThrow();
            } catch (NullPointerException success) {}
        }
    }

    /**
     * timed invokeAll(,,null) throws NPE
     */
    public void testTimedInvokeAllNullTimeUnit() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<>();
            l.add(new StringTask());
            try {
                e.invokeAll(l, randomTimeout(), null);
                shouldThrow();
            } catch (NullPointerException success) {}
        }
    }

    /**
     * timed invokeAll(empty collection) returns empty list
     */
    public void testTimedInvokeAll2() throws Exception {
        final ExecutorService e = createCronScheduler();
        final Collection<Callable<String>> emptyCollection
                = Collections.emptyList();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Future<String>> r =
                    e.invokeAll(emptyCollection, randomTimeout(), randomTimeUnit());
            assertTrue(r.isEmpty());
        }
    }

    /**
     * timed invokeAll(c) throws NPE if c has null elements
     */
    public void testTimedInvokeAll3() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<>();
            l.add(new StringTask());
            l.add(null);
            try {
                e.invokeAll(l, randomTimeout(), randomTimeUnit());
                shouldThrow();
            } catch (NullPointerException success) {}
        }
    }

    /**
     * get of element of invokeAll(c) throws exception on failed task
     */
    public void testTimedInvokeAll4() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<>();
            l.add(new NPETask());
            List<Future<String>> futures =
                    e.invokeAll(l, LONG_DELAY_MS, MILLISECONDS);
            assertEquals(1, futures.size());
            try {
                futures.get(0).get();
                shouldThrow();
            } catch (ExecutionException success) {
                assertTrue(success.getCause() instanceof NullPointerException);
            }
        }
    }

    /**
     * timed invokeAll(c) returns results of all completed tasks
     */
    public void testTimedInvokeAll5() throws Exception {
        final ExecutorService e = createCronScheduler();
        try (PoolCleaner cleaner = cleaner(e)) {
            List<Callable<String>> l = new ArrayList<>();
            l.add(new StringTask());
            l.add(new StringTask());
            List<Future<String>> futures =
                    e.invokeAll(l, LONG_DELAY_MS, MILLISECONDS);
            assertEquals(2, futures.size());
            for (Future<String> future : futures)
                assertSame(TEST_STRING, future.get());
        }
    }

    /**
     * timed invokeAll(c) cancels tasks not completed by timeout
     */
    public void testTimedInvokeAll6() throws Exception {
        for (long timeout = timeoutMillis();;) {
            final CountDownLatch done = new CountDownLatch(1);
            final Callable<String> waiter = new CheckedCallable<String>() {
                public String realCall() {
                    try { done.await(LONG_DELAY_MS, MILLISECONDS); }
                    catch (InterruptedException ok) {}
                    return "1"; }};
            final ExecutorService p = createCronScheduler();
            try (PoolCleaner cleaner = cleaner(p, done)) {
                List<Callable<String>> tasks = new ArrayList<>();
                tasks.add(new StringTask("0"));
                tasks.add(waiter);
                long startTime = System.currentTimeMillis();
                List<Future<String>> futures =
                        p.invokeAll(tasks, timeout, MILLISECONDS);
                assertEquals(tasks.size(), futures.size());
                assertTrue(millisElapsedSince(startTime) >= timeout);
                for (Future future : futures)
                    assertTrue(future.isDone());
                assertTrue(futures.get(1).isCancelled());
                try {
                    assertEquals("0", futures.get(0).get());
                    break;
                } catch (CancellationException retryWithLongerTimeout) {
                    timeout *= 2;
                    if (timeout >= LONG_DELAY_MS / 2)
                        fail("expected exactly one task to be cancelled");
                }
            }
        }
    }

}

