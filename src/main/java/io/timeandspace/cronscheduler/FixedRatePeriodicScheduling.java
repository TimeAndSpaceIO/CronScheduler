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

import net.jcip.annotations.NotThreadSafe;

import java.time.Clock;

import static io.timeandspace.cronscheduler.MathUtils.roundDown;
import static io.timeandspace.cronscheduler.MathUtils.roundUp;

@NotThreadSafe
final class FixedRatePeriodicScheduling<V> extends PeriodicScheduling<V> {

    private final long periodMillis;

    /**
     * If true, {@link #call()} skips scheduling times lagging behind the current
     * time (according to {@link #timeProvider}) until only one scheduled time is left.
     */
    private final boolean skippingToLatest;

    /**
     * Unlike the similar field in JSR-166's ScheduledThreadPoolExecutor.ScheduledFutureTask,
     * this field doesn't need to be volatile because there is only a single thread in this
     * version of ScheduledThreadPoolExecutor.
     */
    private long nextScheduledRunTimeMillis;

    FixedRatePeriodicScheduling(Clock timeProvider, CronTask task, long triggerTimeMillis,
            long periodMillis, boolean skippingToLatest) {
        super(timeProvider, task);
        this.periodMillis = periodMillis;
        this.skippingToLatest = skippingToLatest;
        this.nextScheduledRunTimeMillis = triggerTimeMillis;
    }

    @Override
    public V call() throws Exception {
        if (skippingToLatest) {
            final long lagMillis = timeProvider.millis() - nextScheduledRunTimeMillis;
            if (lagMillis >= periodMillis) {
                long skipMillis = roundDown(lagMillis, periodMillis);
                nextScheduledRunTimeMillis += skipMillis;
            }
        }
        task.run(nextScheduledRunTimeMillis);
        return null;
    }

    @Override
    public void setNextRunTime() {
        nextScheduledRunTimeMillis += periodMillis;
    }

    @Override
    public boolean rewind(long newTimeMillis) {
        if (nextScheduledRunTimeMillis <= newTimeMillis) {
            return false;
        }
        nextScheduledRunTimeMillis -=
                roundUp(nextScheduledRunTimeMillis - newTimeMillis, periodMillis);
        return true;
    }

    @Override
    public long nextScheduledRunTimeMillis() {
        return nextScheduledRunTimeMillis;
    }
}
