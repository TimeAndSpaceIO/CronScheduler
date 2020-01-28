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

import org.jetbrains.annotations.NotNull;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * {@code CronScheduler} is a facility to schedule periodic or one-shot tasks at specified times.
 * The main difference between {@code CronScheduler} and {@link
 * java.util.concurrent.ScheduledThreadPoolExecutor} is that {@code CronScheduler} uses {@link
 * System#currentTimeMillis() currentTimeMillis} as the source of time (or any other time provider
 * {@linkplain CronSchedulerBuilder#setTimeProvider if configured}), while {@link
 * java.util.concurrent.ScheduledThreadPoolExecutor} uses {@link System#nanoTime() nanoTime}. Using
 * {@code currentTimeMillis} as the source of time instead of {@code nanoTime} allows to protect
 * against unbounded clock drift and to accommodate for system time corrections, either manual, or
 * initiated by the NTP server. This makes {@code CronScheduler} similar to {@link java.util.Timer}.
 * However, {@link java.util.Timer} has a limited API and doesn't accommodate for significant
 * system time shifts, as well as machine sleep or hibernation events. {@code CronScheduler} allows
 * to mitigate the schedule disruptions which might be caused by such events via setting
 * sufficiently small <i>sync period</i> (see below for more details).
 *
 * <p>Additional features of {@code CronScheduler}:
 * <ul>
 *     <li>{@link #scheduleAtRoundTimesInDay(Duration, ZoneId, CronTask) scheduleAtRoundTimesInDay}
 *     (and its {@linkplain #scheduleAtRoundTimesInDaySkippingToLatest "skipping to latest"
 *     counterpart}) encapsulate the complexity of calculating the initial delay for a task so that
 *     the first run time is a round wall clock time with respect to the specified period, as well
 *     as maintaining to the wall clock time-driven schedule in the face of daylight saving time
 *     changes.</li>
 *     <li>"Skipping to latest" methods, {@link #scheduleAtFixedRateSkippingToLatest
 *     scheduleAtFixedRateSkippingToLatest} and {@link
 *     #scheduleAtRoundTimesInDaySkippingToLatest scheduleAtRoundTimesInDaySkippingToLatest} prune
 *     excessive runs in the face of observed abrupt forward time shifts. See below for more details
 *     on this.</li>
 * </ul>
 *
 * <p><b>{@code CronScheduler} is single-threaded.</b> It is not meant to be used as a general
 * executor, but rather to provide the scheduling only. The tasks should be kept small and
 * non-blocking. Heavy work should be preferably delegated to other executors.
 *
 * <p>The interface is similar to {@link ScheduledExecutorService}, but because of some differences
 * this interface is not implemented by {@code CronScheduler}; an adapter is provided via {@link
 * #asScheduledExecutorService()}.
 *
 * <h3>Sync period</h3>
 * Every {@code CronScheduler} has a <i>sync period</i> set at construction time. Sync period is the
 * maximum "asleep" period for the {@code CronScheduler}. With at most this period, {@code
 * CronScheduler} checks if the current time changed and readjusts the waiting times for the
 * scheduled tasks if needed. Possible reasons for readjustment are {@linkplain
 * CronSchedulerBuilder#setBackwardTimeShiftLogger backward time shifts}, abrupt forward time shifts
 * (see below), or slow clock drift.
 *
 * <p>On machines that may go to sleep/hibernation (like phones, tablets, PCs, laptops), sync period
 * is the maximum time that will take {@code CronScheduler} to notice the sleep event, and therefore
 * sync period is effectively the upper bound for how much late the scheduled tasks can be run (not
 * considering factors that are outside of the control of {@code CronScheduler}, such as GC pauses).
 * For example, if the CronScheduler is used to notify the user about something every hour, and the
 * user sends the laptop to sleep from 15:01 to 16:01 (just one minute past the last notification),
 * a {@code CronScheduler} with sync period of 5 minutes will trigger the notification at 16:06 the
 * next time. If a {@link java.util.concurrent.ScheduledThreadPoolExecutor} or a {@link
 * java.util.Timer} was used for this task, the notification wouldn't trigger until 17:00, or 59
 * minutes after the laptop has waken up.
 *
 * <p>If {@code CronScheduler} is used in the server environment where machines don't go to
 * sleep or hibernation, sync period should be chosen considering the maximum tolerable clock drift
 * amount (which will then be the upper bound on how much late the scheduled tasks can be run) and
 * the clock drift rate that may conceivably happen in the system (10%, perhaps, or less than that
 * if there are reasons to think there is a stricter upper bound on the clock drift rate). For
 * example, if it's not tolerable that tasks run more than 30 seconds late, the sync period for the
 * {@code CronScheduler} could be chosen at 5 minutes. If at least one periodic task scheduled with
 * the {@code CronScheduler} will have a shorter period, or clock drift is generally not a concern,
 * the sync period could be chosen to be equal to the shortest scheduling period for the tasks.
 *
 * <h3>Skipping to latest run times</h3>
 * <p>{@link #scheduleAtFixedRateSkippingToLatest scheduleAtFixedRateSkippingToLatest} and {@link
 * #scheduleAtRoundTimesInDaySkippingToLatest scheduleAtRoundTimesInDaySkippingToLatest} methods
 * skip scheduled times if there is at least one later scheduled time that has already come with
 * respect to {@link System#currentTimeMillis() currentTimeMillis}, or a different time provider
 * {@linkplain CronSchedulerBuilder#setTimeProvider if configured}. In the case of observed abrupt
 * forward time shift, the task scheduled for periodic run using one of the {@code *SkippingToLatest
 * } methods won't be called with the scheduled times (see {@link CronTask#run CronTask.run}) that
 * already appear outdated. For example, if some task is scheduled with a 10 minute period and at
 * some point there is an apparent 1 hour gap between executions (the previous execution scheduled
 * at 17:00 has finished at 17:02, and the next observed clock time is 18:02), 5 non-latest run
 * times will be skipped: the next time {@link CronTask#run} will be called with {@code
 * scheduledRunTimeMillis} corresponding to 18:00 (rather than 17:10) as the argument.
 *
 * <p>Possible reasons for observed abrupt forward time shift:
 * <ul>
 *     <li>Some of the tasks on this CronScheduler taking long time. (Note that they should be
 *     preferably kept as short as possible.)</li>
 *     <li>CronScheduler is overloaded with tasks.</li>
 *     <li>Application or system overload.</li>
 *     <li>JVM pause (e. g. GC pause) or system pause (e. g. for defragmentation).</li>
 *     <li>System hibernation or sleep.</li>
 *     <li>Actually, a forward time shift, initiated either manually or by an NTP server.</li>
 * </ul>
 *
 * <p>Note that zone offset changes (e. g. DST changes) in the given time zone are <i>not</i> one of
 * the possible reasons, because zone offset governs the translation from a time instant ({@link
 * Instant}) to local time ({@link ZonedDateTime}), whereas the abrupt time shifts {@link
 * #scheduleAtFixedRateSkippingToLatest scheduleAtFixedRateSkippingToLatest} and {@link
 * #scheduleAtRoundTimesInDaySkippingToLatest scheduleAtRoundTimesInDaySkippingToLatest} methods
 * additionally take into consideration (compared to {@link
 * #scheduleAtFixedRate(long, long, TimeUnit, CronTask) scheduleAtFixedRate} and {@link
 * #scheduleAtRoundTimesInDay(Duration, ZoneId, CronTask) scheduleAtRoundTimesInDay}) are the
 * <i>time instant</i> shifts. However, {@link #scheduleAtRoundTimesInDaySkippingToLatest
 * scheduleAtRoundTimesInDaySkippingToLatest} maintains the promise of {@link
 * #scheduleAtRoundTimesInDay scheduleAtRoundTimesInDay} to call {@link CronTask} only with
 * {@code scheduledRunTimeMillis} arguments that represent round wall clock times in the given time
 * zone, whatever instant time shifts or zone offset changes occur in whatever order.
 *
 * <p>In any case, when {@link CronTask} is called, the {@code scheduledRunTimeMillis} provided into
 * it is still not guaranteed to be equal to the current millisecond instant, or being not more late
 * than the scheduling period of the task, or the sync period of the {@code CronScheduler}. It's
 * fundamentally impossible to provide such a guarantee because an arbitrarily long pause may always
 * happen between the check and the actual logic. The primary use case for {@code *SkippingToLatest}
 * methods is protecting an upstream object, system, or service from <i>request flood (aka query
 * storm) after pauses</i>, without requiring {@link CronTask} implementations to filter non-latest
 * scheduled runs manually. The timeliness of requests should be checked on the receiver side if
 * required.
 *
 * <h3>Handling of backward time shifts</h3>
 * <p>Whenever some task is due for execution, or at least every sync period {@code CronScheduler}
 * checks if the current time is before the previously observed time. If this is the case, {@code
 * CronScheduler} first logs the backward time shift occurrence (to {@code System.err} by default,
 * or to a custom logger {@linkplain CronSchedulerBuilder#setBackwardTimeShiftLogger if
 * configured}). Then, {@code CronScheduler} ensures that all periodic tasks will run properly with
 * respect to the new current time.
 *
 * <p>Periodic tasks created with all {@code scheduleAtFixedRate} and {@code
 * scheduleAtRoundTimesInDay} methods are assumed to extend indefinitely in the past if the time
 * is shifted before the initial trigger time of the task. The past scheduling times will be {@code
 * initialDelay - period}, {@code initialDelay - 2 * period}, and so on for {@code
 * scheduleAtFixedRate} methods (but not that a negative {@code initialDelay} is always replaced
 * with zero up front), and past round clock times within a day for {@code
 * scheduleAtRoundTimesInDay} methods. If this is undesirable, the past run times could be filtered
 * manually in the definition of the {@link CronTask}: <pre>{@code
 * long currentTime = System.currentTimeMillis();
 * CronTask task = scheduledRunTimeMillis -> {
 *   if (scheduledRunTimeMillis >= currentTime) {
 *     // perform the task
 *   }
 * };
 * cronScheduler.scheduleAtFixedRate(0, 1, MINUTES, task);}</pre>
 */
public class CronScheduler extends ScheduledThreadPoolExecutor {

    static final Duration MIN_SYNC_PERIOD = Duration.ofSeconds(1);
    private static final int MIN_ROUND_TIMES_IN_DAY_PERIOD_SECONDS = 5;
    private static final long SECONDS_IN_DAY = TimeUnit.DAYS.toSeconds(1);
    static final Duration MAX_SYNC_PERIOD = Duration.ofDays(1);

    /**
     * Returns true if the scheduler thread is currently running.
     *
     * @return true if the scheduler thread is currently running
     * @see #prestartThread()
     * @see #isTerminated()
     */
    public boolean isThreadRunning() {
        return super.isThreadRunning();
    }

    static void checkSyncPeriod(Duration syncPeriod) {
        Objects.requireNonNull(syncPeriod);
        if (syncPeriod.compareTo(MIN_SYNC_PERIOD) < 0 ||
                syncPeriod.compareTo(MAX_SYNC_PERIOD) > 0) {
            throw new IllegalArgumentException(
                    String.format("syncPeriod is outside of allowed range [%s, %s]: %s",
                            MIN_SYNC_PERIOD, MAX_SYNC_PERIOD, syncPeriod));
        }
    }

    static final Consumer<String> DEFAULT_BACKWARD_TIME_SHIFT_LOGGER =
            message -> {
                // Use "late binding" of System.err because it may be reset by the application at
                // any time.
                //noinspection Convert2MethodRef
                System.err.println(message);
            };

    /**
     * Creates and returns a new {@link CronSchedulerBuilder} which can be used to build {@code
     * CronScheduler}s with the given <i>sync period</i>. See more details about sync period and
     * how it should be chosen in the class-level documentation for {@code CronScheduler}.
     *
     * @param syncPeriod the sync period for {@code CronScheduler}s to be built with the returned
     *        builder
     * @return a new {@link CronSchedulerBuilder}
     * @throws NullPointerException if the sync period is null
     * @throws IllegalArgumentException if sync period is less than 1 second or more than 1 day
     */
    public static CronSchedulerBuilder newBuilder(Duration syncPeriod) {
        return new CronSchedulerBuilder(syncPeriod);
    }

    /**
     * Creates and returns a new {@code CronScheduler} with the given sync period. See more details
     * about sync period and how it should be chosen in the class-level documentation for {@code
     * CronScheduler}. Other settings are equal to the defaults of {@link CronSchedulerBuilder}.
     * {@link System#currentTimeMillis() currentTimeMillis} is used as the time source. The sole
     * thread of the {@code CronScheduler} will be a daemon thread of {@linkplain
     * Thread#NORM_PRIORITY normal priority}, not tied to any {@link ThreadGroup}, with a name of the
     * form "cron-scheduler-N" where N is a global sequence number. Backward time shift events will
     * be logged into {@link System#err} if observed.
     *
     * @param syncPeriod the sync period for {@code CronScheduler}
     * @return a new {@code CronScheduler}
     * @throws NullPointerException if the sync period is null
     * @throws IllegalArgumentException if sync period is less than 1 second or more than 1 day
     */
    public static CronScheduler create(Duration syncPeriod) {
        return new CronScheduler(Clock.systemUTC(), syncPeriod,
                DefaultCronSchedulerThreadFactory.INSTANCE);
    }

    private CronScheduler(Clock clock, Duration syncPeriod, ThreadFactory threadFactory) {
        super(clock, syncPeriod, threadFactory, DEFAULT_BACKWARD_TIME_SHIFT_LOGGER);
    }

    CronScheduler(CronSchedulerBuilder builder) {
        super(builder.getTimeProvider(), builder.getSyncPeriod(), builder.getThreadFactory(),
                builder.getBackwardTimeShiftLogger());
    }

    /**
     * Submits a one-shot task that becomes enabled at the given instant.
     *
     * @param triggerTime the moment of time when the task should be fired for execution
     *        ({@link Clock#millis()} of the time provider used by this CronScheduler should become
     *        greater than or equal to {@link Instant#toEpochMilli()} of the given trigger time)
     * @param command the task to be executed
     * @return a {@link Future} representing pending completion of the task and whose {@code get()}
     *         method will return {@code null} upon completion
     * @throws RejectedExecutionException if this CronScheduler has already been shut down
     * @throws NullPointerException if triggerTime or command is null
     */
    public Future<?> scheduleAt(Instant triggerTime, Runnable command) {
        Objects.requireNonNull(triggerTime);
        Objects.requireNonNull(command);
        ScheduledFutureTask<Void> t =
                new ScheduledFutureTask<>(command, null, triggerTime.toEpochMilli());
        delayedExecute(t);
        return t;
    }

    /**
     * Submits a one-shot task that becomes enabled at the given instant.
     *
     * @param triggerTime the moment of time when the task should be fired for execution
     *        ({@link Clock#millis()} of the time provider used by this CronScheduler should become
     *        greater than or equal to {@link Instant#toEpochMilli()} of the given trigger time)
     * @param callable the task to be executed
     * @param <V> the type of the callable's result
     * @return a {@link Future} representing pending completion of the task
     * @throws RejectedExecutionException if this CronScheduler has already been shut down
     * @throws NullPointerException if triggerTime or callable is null
     */
    public <V> Future<V> scheduleAt(Instant triggerTime, Callable<V> callable) {
        Objects.requireNonNull(triggerTime);
        Objects.requireNonNull(callable);
        ScheduledFutureTask<V> t = new ScheduledFutureTask<>(callable, triggerTime.toEpochMilli());
        delayedExecute(t);
        return t;
    }

    /**
     * Submits a periodic task that becomes enabled at round clock times within a day, with the
     * given period, in the given time zone. For example, if the current time (according to the
     * {@link System#currentTimeMillis() currentTimeMillis}, or a custom time provider {@linkplain
     * CronSchedulerBuilder#setTimeProvider if configured}) in the given time zone is 00:26 and the
     * period of 1 hour is given, the task will be first executed in 34 minutes, then in 1 hour 34
     * minutes, etc. In other words, it would be equivalent to a call to {@link
     * #scheduleAtFixedRate(long, long, TimeUnit, CronTask)
     * scheduleAtFixedRate(34, 60, MINUTES, task)} (as long as the time zone doesn't change the
     * offset).
     *
     * <p>When daylight saving time or permanent offset changes occur in the given time zone, this
     * method preserves the affinity of the schedule to the round clock times in the day, which may
     * result in a shorter or longer than nominal period (in actual, physical time terms) between
     * two consecutive runs of the task when the clock is changed. When the clock in the given time
     * zone is changed backward at a time which is also a round time for the task call (for example,
     * the period is 2 hours, and the clock is changed at 02:00), {@link CronTask} will be run two
     * consecutive times at the same local wall clock time, with a period between these runs equal
     * to the zone offset change amount (if this is a DST change, it's usually 1 hour).
     *
     * <p>At the moment when a zone offset change occurs (such as a DST change), the task is
     * triggered only if the local wall clock time <i>after</i> the transition is divisible by the
     * given period, but <i>not</i> if the local wall clock time <i>before</i> the transition is
     * divisible by the given period. For example, if the period is 2 hours, and there is a DST
     * change from 02:00 to 03:00, the task will be called only at the local times 00:00 and then
     * 04:00, but not 02:00. This is because the local wall clock time before the transition is not
     * actually considered valid by convention. See the documentation for {@link
     * java.time.zone.ZoneOffsetTransition#getDateTimeBefore} and {@link
     * java.time.zone.ZoneOffsetTransition#getDateTimeAfter}.
     *
     * <p>If irregularity in physical time periods between the task runs in the face of zone offset
     * changes (e. g. DST changes) is not wanted, it can be avoided by passing a {@link
     * ZoneOffset} into this method instead of a region-based time zone. The current zone offset
     * could be obtained from the region-based id as follows: <pre>{@code
     * ZoneId regionZone = ZoneId.systemDefault();
     * ZoneOffset zoneOffset = regionZone.getRules().getOffset(Instant.now(clock));}</pre>
     *
     * <p>This method is only as accurate with respect to zone offset transitions as the given
     * region-based {@link ZoneId} is. In practice, it means that if one of the standard time zones
     * with the default (static) {@link java.time.zone.ZoneRulesProvider} is used, and then the
     * government of the territory decides to introduce or abolish daylight saving time, or to
     * change the zone offset permanently, the ongoing call to {@code scheduleAtRoundTimesInDay}
     * won't pick up these changes. In order to enable this method to take into account arbitrary
     * future {@link java.time.zone.ZoneRules} changes, a {@link ZoneId} linked to a dynamic {@link
     * java.time.zone.ZoneRulesProvider} should be used.
     *
     * <p>Note that the time zone from the time provider {@linkplain
     * CronSchedulerBuilder#setTimeProvider configured for the CronScheduler} ({@link
     * Clock#getZone}) is <i>not</i> taken into account.
     *
     * <p>If the time zone distinction is not important, or if 15 minutes are divisible by the
     * period of the task (like 5 minutes or 15 minutes; it makes the scheduling of the task
     * effectively indifferent to any real-world time zone changes) {@link java.time.ZoneOffset#UTC}
     * or {@link ZoneId#systemDefault()} could be passed into this method.
     *
     * <p>When a backward time shift occurs, the task might be executed at times preceding the first
     * run time of the task, as long as they represent round clock times within a day. See more
     * details about backward time shifts in the class-level documentation for {@code
     * CronScheduler}.
     *
     * @param period the wall clock period between successive executions
     * @param zoneId time zone used to calculate the initial delay for the task and to adjust
     *        the delay when zone offset changes (DST or permanent) happen
     * @param task the task to be executed
     * @return a {@link Future} representing pending completion of
     *         the series of repeated tasks.  The future's {@link
     *         Future#get() get()} method will never return normally,
     *         and will throw an exception upon task cancellation or
     *         abnormal termination of a task execution.
     * @throws RejectedExecutionException if this CronScheduler has already been shut down
     * @throws NullPointerException if period, zoneId, or task is null
     * @throws IllegalArgumentException if the period is less than 5 seconds, or is not an integral
     *         part of a day, or is greater than a day
     * @see #scheduleAtFixedRate(long, long, TimeUnit, CronTask)
     * @see #scheduleAtRoundTimesInDaySkippingToLatest(Duration, ZoneId, CronTask)
     */
    public Future<?> scheduleAtRoundTimesInDay(Duration period, ZoneId zoneId, CronTask task) {
        return scheduleAtRoundTimesInDay(period, zoneId, task, false);
    }

    /**
     * Submits a periodic task that becomes enabled at round clock times within a day, with the
     * given period, in the given time zone, skipping scheduled times if there is at least one other
     * later scheduled time that has already come. This method behaves the same as {@link
     * #scheduleAtRoundTimesInDay(Duration, ZoneId, CronTask) scheduleAtRoundTimesInDay}, except
     * that in case of observed abrupt forward time shift, the task is not called provided with the
     * scheduling times (see {@link CronTask#run CronTask.run}) that already appear outdated. See
     * more details about "skipping to latest" concept in the class-level documentation for {@code
     * CronScheduler}.
     *
     * <p>This method maintains the promise of {@link
     * #scheduleAtRoundTimesInDay(Duration, ZoneId, CronTask) scheduleAtRoundTimesInDay} to call
     * {@link CronTask} only with {@code scheduledRunTimeMillis} arguments that represent round wall
     * clock times in the given time zone, whatever instant time shifts or zone offset changes occur
     * in whatever order.
     *
     * @param period the wall clock period between successive executions
     * @param zoneId time zone used to calculate the initial delay for the task and to adjust the
     *        delay when zone offset changes (DST or permanent) happen
     * @param task the task to be executed
     * @return a {@link Future} representing pending completion of
     *         the series of repeated tasks.  The future's {@link
     *         Future#get() get()} method will never return normally,
     *         and will throw an exception upon task cancellation or
     *         abnormal termination of a task execution.
     * @throws RejectedExecutionException if this CronScheduler has already been shut down
     * @throws NullPointerException if period, zoneId, or task is null
     * @throws IllegalArgumentException if the period is less than 5 seconds, or is not an integral
     *         number of seconds, or is not an integral part of a day, or is greater than a day
     * @see #scheduleAtFixedRateSkippingToLatest
     * @see #scheduleAtRoundTimesInDay(Duration, ZoneId, CronTask)
     */
    public Future<?> scheduleAtRoundTimesInDaySkippingToLatest(Duration period, ZoneId zoneId,
            CronTask task) {
        return scheduleAtRoundTimesInDay(period, zoneId, task, true);
    }

    Future<?> scheduleAtRoundTimesInDay(Duration period, ZoneId zoneId, CronTask task,
            boolean skippingToLatest) {
        Objects.requireNonNull(period);
        Objects.requireNonNull(zoneId);
        Objects.requireNonNull(task);
        long periodSeconds = toSeconds(period);
        if (!Duration.ofSeconds(periodSeconds).equals(period)) {
            throw new IllegalArgumentException(
                    period + " should be an integral number of seconds");
        }
        if (periodSeconds < MIN_ROUND_TIMES_IN_DAY_PERIOD_SECONDS ||
                SECONDS_IN_DAY % periodSeconds != 0) {
            throw new IllegalArgumentException(
                    "period should be no less than " + MIN_ROUND_TIMES_IN_DAY_PERIOD_SECONDS +
                            " seconds and be an integral part of a day, " +
                            period + " given");
        }
        final PeriodicScheduling<Void> periodicScheduling =
                new RoundWallTimeInDayPeriodicScheduling<>(timeProvider, task, zoneId, periodSeconds,
                        skippingToLatest);
        final ScheduledFutureTask<Void> sft = new ScheduledFutureTask<>(periodicScheduling);
        delayedExecute(sft);
        return sft;
    }

    private static long toSeconds(Duration period) {
        return MILLISECONDS.toSeconds(period.toMillis());
    }

    /**
     * Submits a periodic action that becomes enabled first after the
     * given initial delay, and subsequently with the given period;
     * that is, executions will commence after
     * {@code initialDelay}, then {@code initialDelay + period}, then
     * {@code initialDelay + 2 * period}, and so on.
     *
     * <p>A negative {@code initialDelay} value is replaced with zero to determine all future (or
     * prior, which may be relevant when backward time shifts occur; see below) scheduling times.
     *
     * <p>When a backward time shift occurs, the task might be executed at the times preceding
     * the initial trigger time: {@code initialDelay - period}, {@code initialDelay - 2 * period},
     * and so on. See more details about backward time shifts in the class-level documentation for
     * {@code CronScheduler}.
     *
     * <p>The sequence of task executions continues indefinitely until
     * one of the following exceptional completions occur:
     * <ul>
     * <li>The task is {@linkplain Future#cancel explicitly cancelled}
     * via the returned future.
     * <li>{@link #shutdown shutdown()} or {@link #shutdownNow} is called; also resulting in task
     * cancellation.
     * <li>An execution of the task throws an exception.  In this case
     * calling {@link Future#get() get} on the returned future will throw
     * {@link ExecutionException}, holding the exception as its cause.
     * </ul>
     * Subsequent executions are suppressed.  Subsequent calls to
     * {@link Future#isDone isDone()} on the returned future will
     * return {@code true}.
     *
     * @param task the task to execute
     * @param initialDelay the time to delay first execution
     * @param period the period between successive executions
     * @param unit the time unit of the initialDelay and period parameters
     * @return a {@link Future} representing pending completion of
     *         the task, and whose {@code get()} method will throw an
     *         exception upon cancellation
     * @throws RejectedExecutionException if this CronScheduler has already been shut down
     * @throws NullPointerException if the unit or the task is null
     * @throws IllegalArgumentException if period less than or equal to zero, or if the initial
     *         delay converted to milliseconds is less than {@code - (Long.MAX_VALUE / 2)} or is
     *         greater than {@code Long.MAX_VALUE / 2}
     */
    public Future<?> scheduleAtFixedRate(long initialDelay, long period, TimeUnit unit,
            CronTask task) {
        return scheduleAtFixedRate(initialDelay, period, unit, task, false);
    }

    /**
     * Submits a periodic action that becomes enabled first after the
     * given initial delay, and subsequently with the given period;
     * that is, executions will commence after
     * {@code initialDelay}, then {@code initialDelay + period}, then
     * {@code initialDelay + 2 * period}, and so on; scheduled times will be skipped if there is at
     * least one other later scheduled time that has already come. This method behaves the same as
     * {@link #scheduleAtFixedRate(long, long, TimeUnit, CronTask) scheduleAtFixedRate}, except
     * that in case of observed abrupt forward time shift, the task is not called provided with the
     * scheduling times (see {@link CronTask#run CronTask.run}) that already appear outdated. See
     * more details about "skipping to latest" concept in the class-level documentation for {@code
     * CronScheduler}.
     *
     * @param task the task to execute
     * @param initialDelay the time to delay first execution
     * @param period the period between successive executions
     * @param unit the time unit of the initialDelay and period parameters
     * @return a {@link Future} representing pending completion of
     *         the task, and whose {@code get()} method will throw an
     *         exception upon cancellation
     * @throws RejectedExecutionException if this CronScheduler has already been shut down
     * @throws NullPointerException if the unit or the task is null
     * @throws IllegalArgumentException if period less than or equal to zero, or if the initial
     *         delay converted to milliseconds is less than {@code - (Long.MAX_VALUE / 2)} or is
     *         greater than {@code Long.MAX_VALUE / 2}
     */
    public Future<?> scheduleAtFixedRateSkippingToLatest(long initialDelay, long period,
            TimeUnit unit, CronTask task) {
        return scheduleAtFixedRate(initialDelay, period, unit, task, true);
    }

    private Future<?> scheduleAtFixedRate(long initialDelay, long period, TimeUnit unit,
            CronTask task, boolean skippingToLatest) {
        Objects.requireNonNull(task);
        Objects.requireNonNull(unit);
        if (unit == TimeUnit.NANOSECONDS || unit == TimeUnit.MICROSECONDS) {
            throw new IllegalArgumentException(
                    "unit must be at least milliseconds, " + unit + " given");
        }
        if (period <= 0L) {
            throw new IllegalArgumentException("period must be positive, " + period + " given");
        }
        long initialDelayMillis = unit.toMillis(initialDelay);
        if (Math.abs(initialDelayMillis) > Long.MAX_VALUE / 2) {
            throw new IllegalArgumentException(
                    "initial delay out of reasonable bounds: " + initialDelay + " " + unit);
        }
        long triggerTimeMillis = Math.max(initialDelayMillis, 0) + timeProvider.millis();
        final PeriodicScheduling<Void> periodicScheduling = new FixedRatePeriodicScheduling<>(
                timeProvider, task, triggerTimeMillis, unit.toMillis(period), skippingToLatest);
        ScheduledFutureTask<Void> sft = new ScheduledFutureTask<>(periodicScheduling);
        delayedExecute(sft);
        return sft;
    }

    /**
     * Returns an adapter of this {@code CronScheduler} to {@link ScheduledExecutorService}
     * interface. The returned {@link ScheduledExecutorService} acts the same as a newly constructed
     * {@link java.util.concurrent.ScheduledThreadPoolExecutor} with one thread, {@link
     * java.util.concurrent.ThreadPoolExecutor.AbortPolicy} as the {@link
     * java.util.concurrent.RejectedExecutionHandler}, and none {@code setXxx()} methods called,
     * i. e. using the default behaviour: stopping all periodic tasks after a shutdown, but still
     * executing all one-shot tasks that have already been scheduled.
     *
     * <p>The returned {@link ScheduledExecutorService} does not support {@link
     * ScheduledExecutorService#scheduleWithFixedDelay scheduleWithFixedDelay} operation: an {@link
     * UnsupportedOperationException} is thrown when this method is called.
     *
     * @return a view of this CronScheduler as a {@link ScheduledExecutorService}
     */
    public ScheduledExecutorService asScheduledExecutorService() {
        return new ScheduledExecutorServiceAdapter();
    }

    /**
     * Initiates an orderly shutdown in which previously submitted
     * tasks are executed, but no new tasks will be accepted.
     * Invocation has no additional effect if already shut down.
     *
     * <p>This method does not wait for previously submitted tasks to
     * complete execution.  Use {@link #awaitTermination awaitTermination}
     * to do that.
     *
     * @param oneShotTasksShutdownPolicy the policy determining whether delayed one-shot tasks
     *        should be executed
     * @throws SecurityException if a security manager exists and
     *         shutting down this CronScheduler may manipulate threads that the caller is not
     *         permitted to modify because it does not hold {@link
     *         java.lang.RuntimePermission}{@code ("modifyThread")},
     *         or the security manager's {@code checkAccess} method denies access.
     */
    @Override
    public void shutdown(OneShotTasksShutdownPolicy oneShotTasksShutdownPolicy) {
        super.shutdown(oneShotTasksShutdownPolicy);
    }

    private class ScheduledExecutorServiceAdapter extends AbstractExecutorService
            implements ScheduledExecutorService {

        @Override
        public void execute(@NotNull Runnable command) {
            scheduleAt(timeProvider.instant(), command);
        }

        /**
         * Using {@link OneShotTasksShutdownPolicy#EXECUTE_DELAYED} because the default for {@link
         * java.util.concurrent.ScheduledThreadPoolExecutor#getExecuteExistingDelayedTasksAfterShutdownPolicy()}
         * is true.
         */
        @Override
        public void shutdown() {
            CronScheduler.this.shutdown(OneShotTasksShutdownPolicy.EXECUTE_DELAYED);
        }

        /**
         * All elements in the returned list are {@link
         * io.timeandspace.cronscheduler.ScheduledThreadPoolExecutor.ScheduledFutureTask}s, so
         * casting to List of Runnable is safe. There is also no "array assignment problem"
         * because the list is unmodifiable (see {@link ThreadPoolExecutor}.drainQueue()).
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        @NotNull
        @Override
        public List<Runnable> shutdownNow() {
            return (List) CronScheduler.this.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return CronScheduler.this.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return CronScheduler.this.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, @NotNull TimeUnit unit)
                throws InterruptedException {
            return CronScheduler.this.awaitTermination(timeout, unit);
        }

        @NotNull
        @Override
        public ScheduledFuture<?> schedule(@NotNull Runnable command, long delay,
                @NotNull TimeUnit unit) {
            return (ScheduledFutureTask<?>) scheduleAt(
                    timeProvider.instant().plus(unit.toNanos(delay), ChronoUnit.NANOS),
                    command);
        }

        @NotNull
        @Override
        public <V> ScheduledFuture<V> schedule(@NotNull Callable<V> callable, long delay,
                @NotNull TimeUnit unit) {
            return (ScheduledFutureTask<V>) scheduleAt(
                    timeProvider.instant().plus(unit.toNanos(delay), ChronoUnit.NANOS),
                    callable);
        }

        @NotNull
        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(@NotNull Runnable command, long initialDelay,
                long period, @NotNull TimeUnit unit) {
            Objects.requireNonNull(command);
            return (ScheduledFutureTask<?>) CronScheduler.this.scheduleAtFixedRate(initialDelay,
                    period, unit, t -> command.run());
        }

        @NotNull
        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(@NotNull Runnable command,
                long initialDelay, long delay, @NotNull TimeUnit unit) {
            Objects.requireNonNull(command);
            Objects.requireNonNull(unit);
            throw new UnsupportedOperationException("scheduleWithFixedDelay is not supported");
        }

        @Override
        public String toString() {
            return "ScheduledExecutorServiceAdapter[" + CronScheduler.this.toString() + "]";
        }
    }
}
