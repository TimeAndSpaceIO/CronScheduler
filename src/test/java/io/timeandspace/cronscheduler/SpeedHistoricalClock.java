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

import java.time.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class SpeedHistoricalClock extends Clock implements AutoCloseable {

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor();

    private volatile Instant currentTime;

    public SpeedHistoricalClock(Instant epoch, Duration clockTick, Duration realTimeTick) {
        this.currentTime = epoch;
        executor.scheduleAtFixedRate(() -> {
            currentTime = currentTime.plus(clockTick);
        }, realTimeTick.toNanos(), realTimeTick.toNanos(), TimeUnit.NANOSECONDS);
    }

    void insertRewind(Duration duration) {
        executor.submit(() -> {
            currentTime = currentTime.minus(duration);
        });
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
        return currentTime;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
