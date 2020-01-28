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

import org.jetbrains.annotations.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

import static io.timeandspace.cronscheduler.CronScheduler.DEFAULT_BACKWARD_TIME_SHIFT_LOGGER;
import static io.timeandspace.cronscheduler.CronScheduler.checkSyncPeriod;

/**
 * A builder for {@link CronScheduler}. Create one using {@link CronScheduler#newBuilder
 * CronScheduler.newBuilder} method.
 */
public class CronSchedulerBuilder {

    private final Duration syncPeriod;
    private @Nullable Integer threadPriority = null;
    private @Nullable String threadName = null;
    private ThreadFactory threadFactory = DefaultCronSchedulerThreadFactory.INSTANCE;
    private Clock timeProvider = Clock.systemUTC();
    private Consumer<String> backwardTimeShiftLogger = DEFAULT_BACKWARD_TIME_SHIFT_LOGGER;

    CronSchedulerBuilder(Duration syncPeriod) {
        checkSyncPeriod(syncPeriod);
        this.syncPeriod = syncPeriod;
    }

    /**
     * Sets the thread priority for the {@link CronScheduler}'s thread. If specified, overwrites the
     * priority set by the {@link ThreadFactory} if any is set via {@link
     * #setThreadFactory(ThreadFactory)}. By default, a thread of {@linkplain Thread#NORM_PRIORITY
     * normal priority} is created.
     *
     * @param priority the priority for the {@link CronScheduler}'s thread
     * @return this builder back
     * @throws IllegalArgumentException if the given priority is less than {@link
     *         Thread#MIN_PRIORITY} or greater than {@link Thread#MAX_PRIORITY}.
     */
    public CronSchedulerBuilder setThreadPriority(int priority) {
        if (priority < Thread.MIN_PRIORITY || priority > Thread.MAX_PRIORITY) {
            throw new IllegalArgumentException(
                    String.format("Priority is outside of allowed range [%d, %d]: %d",
                            Thread.MIN_PRIORITY, Thread.MAX_PRIORITY, priority));
        }
        this.threadPriority = priority;
        return this;
    }

    /**
     * Sets the name for the {@link CronScheduler}'s thread. If specified, overwrites the name set
     * by the {@link ThreadFactory} if any is set via {@link #setThreadFactory(ThreadFactory)}. By
     * default, a name like "cron-scheduler-N" is used, where N is a global sequence number.
     *
     * @param threadName the name for the {@link CronScheduler}'s thread
     * @return this builder back
     * @throws NullPointerException if the given name is null
     */
    public CronSchedulerBuilder setThreadName(String threadName) {
        Objects.requireNonNull(threadName);
        this.threadName = threadName;
        return this;
    }

    /**
     * Sets the thread factory to be used to create the {@link CronScheduler}'s thread. The default
     * thread factory creates daemon threads of {@linkplain Thread#NORM_PRIORITY normal priority},
     * not tied to any {@link ThreadGroup}, with a name of the form "cron-scheduler-N" where N is
     * a global sequence number. If {@link #setThreadName} or {@link #setThreadPriority} are
     * explicitly specified they overwrite the values set by the thread factory.
     *
     * @param factory the thread factory to be used to create the {@link CronScheduler}'s thread.
     * @return this builder back
     * @throws NullPointerException if the given factory is null
     */
    public CronSchedulerBuilder setThreadFactory(ThreadFactory factory) {
        Objects.requireNonNull(factory);
        this.threadFactory = factory;
        return this;
    }

    /**
     * Sets the time provider to be used in {@link CronScheduler}(s) created with this builder. By
     * default, {@link Clock#systemUTC()} is used, which in turn delegates to {@link
     * System#currentTimeMillis() currentTimeMillis}. Note that the time zone embedded into the
     * {@link Clock} is <i>not</i> used by {@link CronScheduler}. Time zone-sensitive methods such
     * as {@link CronScheduler#scheduleAtRoundTimesInDay scheduleAtRoundTimesInDay} accept {@link
     * java.time.ZoneId} as a parameter.
     *
     * @param timeProvider a {@link Clock} instance used to provide the instant time
     * @return this builder back
     * @throws NullPointerException if the given time provider is null
     */
    public CronSchedulerBuilder setTimeProvider(Clock timeProvider) {
        Objects.requireNonNull(timeProvider);
        this.timeProvider = timeProvider;
        return this;
    }

    /**
     * Creates and returns a new {@link CronScheduler} using this builder.
     *
     * @return a new {@link CronScheduler} using parameters configured in this builder
     */
    public CronScheduler build() {
        return new CronScheduler(this);
    }

    /**
     * Sets the logger to log detected backward time shift events. "Backward time shift" means that
     * two calls A and B to {@link Clock#instant()} with a happens-before relation between them
     * return {@code instantA} and {@code instantB} where {@code instantA} is later than {@code
     * instantB}. This may be caused by NTP server-initiated adjustments of the system time on the
     * machine, manual adjustments of the system time on the machine, or leap seconds. Note that
     * local time zone offset changes (e. g. daylight saving time changes) <i>don't</i> cause such
     * time shifts because they affect wall clock representation of the instant times, not the
     * instant times themselves.
     *
     * <p>By default, a lambda equivalent to {@code message -> System.err.println(message)} is used.
     *
     * <p>Example usage: {@code
     * setBackwardTimeShiftLogger(LoggerFactory.getLogger(CronScheduler.class)::info)}.
     *
     * @param logger the logger to log backward time shift events detected by the CronScheduler.
     * @return this builder back
     * @throws NullPointerException if the given logger is null
     */
    public CronSchedulerBuilder setBackwardTimeShiftLogger(Consumer<String> logger) {
        Objects.requireNonNull(logger);
        this.backwardTimeShiftLogger = logger;
        return this;
    }

    Clock getTimeProvider() {
        return timeProvider;
    }

    Duration getSyncPeriod() {
        return syncPeriod;
    }

    ThreadFactory getThreadFactory() {
        if (threadPriority != null || threadName != null) {
            return new OverwritingThreadFactory(threadFactory, threadPriority, threadName);
        } else {
            return threadFactory;
        }
    }

    Consumer<String> getBackwardTimeShiftLogger() {
        return backwardTimeShiftLogger;
    }
}
