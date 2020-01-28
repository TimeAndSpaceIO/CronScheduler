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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

final class TimesRecordingCronTask implements CronTask {
    private final Phaser numRunTimesCounter = new Phaser(1);
    private final int maxTimes;
    final List<Long> runTimes = new ArrayList<>();

    TimesRecordingCronTask(int maxTimes) {
        this.maxTimes = maxTimes;
    }

    @Override
    public void run(long scheduledRunTimeMillis) throws Exception {
        runTimes.add(scheduledRunTimeMillis);
        numRunTimesCounter.arrive();
        if (runTimes.size() == maxTimes) {
            throw new Exception(); // Let the periodic task stop executing
        }
    }

    void awaitNumRunTimes(int n, long timeout, TimeUnit unit)
            throws TimeoutException, InterruptedException {
        for (int currentNumRunTimes; (currentNumRunTimes = numRunTimesCounter.getPhase()) < n;) {
            numRunTimesCounter.awaitAdvanceInterruptibly(currentNumRunTimes, timeout, unit);
        }
    }

    List<ZonedDateTime> getRunTimes(ZoneId zone) {
        return runTimes.stream()
                .map(t -> ZonedDateTime.ofInstant(Instant.ofEpochMilli(t), zone))
                .collect(Collectors.toList());
    }

}
