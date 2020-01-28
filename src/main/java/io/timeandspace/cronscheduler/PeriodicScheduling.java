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

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Part of {@link ScheduledThreadPoolExecutor.ScheduledFutureTask}'s state. Extracted as a separate
 * class to be able to wrap {@link CronTask} as a Callable in the super() constructor call {@link
 * java.util.concurrent.FutureTask#FutureTask(Callable)} in {@link
 * ScheduledThreadPoolExecutor.ScheduledFutureTask#ScheduledFutureTask(PeriodicScheduling)}.
 *
 * @param <V> dummy type, actually always returns null from {@link #call()}
 */
abstract class PeriodicScheduling<V> implements Callable<V> {
    final Clock timeProvider;
    final CronTask task;

    PeriodicScheduling(Clock timeProvider, CronTask task) {
        this.timeProvider = Objects.requireNonNull(timeProvider);
        this.task = Objects.requireNonNull(task);
    }

    @Override
    public abstract V call() throws Exception;

    /**
     * Sets the next time to run for a periodic task.
     */
    abstract void setNextRunTime();

    /**
     * Returns true if rewound and the corresponding {@link
     * ScheduledThreadPoolExecutor.ScheduledFutureTask} potentially needs to be repositioned in the
     * {@link ScheduledThreadPoolExecutor.DelayedWorkQueue}.
     *
     * @param newTimeMillis new time
     * @return true if next run time has been changed
     */
    abstract boolean rewind(long newTimeMillis);

    abstract long nextScheduledRunTimeMillis();
}
