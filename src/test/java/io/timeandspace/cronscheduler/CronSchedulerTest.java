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

import org.awaitility.Awaitility;
import org.junit.Test;

import java.time.*;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static io.timeandspace.cronscheduler.ScheduledExecutorTest.ONE_SEC;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.of;
import static java.util.concurrent.TimeUnit.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class CronSchedulerTest {

    public static final ZoneId LORD_HOWE_TZ = ZoneId.of("Australia/Lord_Howe");

    static AutoCloseable asCloseable(CronScheduler s) {
        return () -> {
            s.shutdown(OneShotTasksShutdownPolicy.DISCARD_DELAYED);
            if (!s.awaitTermination(10, SECONDS)) {
                throw new AssertionError("Failed to terminate CronScheduler");
            }
        };
    }

    @Test
    public void testScheduleAtRoundTimesInDayDst() throws Exception {
        final ZonedDateTime dt = of(
                LocalDateTime.of(2019, 4, 7, 1, 29), LORD_HOWE_TZ);
        final SpeedHistoricalClock clock =
                new SpeedHistoricalClock(Instant.from(dt), Duration.ofMinutes(10), ONE_SEC);
        final CronScheduler s = CronScheduler.newBuilder(ONE_SEC).setTimeProvider(clock).build();
        try (AutoCloseable ignore = asCloseable(s)) {
            final TimesRecordingCronTask task = new TimesRecordingCronTask(4);
            final Future<?> f =
                    s.scheduleAtRoundTimesInDay(Duration.ofMinutes(18), LORD_HOWE_TZ, task);
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(f::isDone);

            final ZonedDateTime firstTime = of(LocalDateTime.of(2019, 4, 7, 1, 30), LORD_HOWE_TZ);
            final List<ZonedDateTime> runTimes = task.getRunTimes(LORD_HOWE_TZ);
            assertThat(runTimes).containsExactly(
                    firstTime,
                    firstTime.plusMinutes(18),
                    firstTime.plusMinutes(30),
                    firstTime.plusMinutes(48)
            );
        }
    }

    @Test
    public void testScheduleAtRoundTimesSkippingToLatest() throws Exception {
        final SettableClock clock = new SettableClock(Instant.EPOCH);
        final CronScheduler s = CronScheduler.newBuilder(ONE_SEC).setTimeProvider(clock).build();
        try (AutoCloseable ignore = asCloseable(s)) {
            final TimesRecordingCronTask task = new TimesRecordingCronTask(2);
            final Future<?> f = s.scheduleAtRoundTimesInDaySkippingToLatest(
                    Duration.ofMinutes(1), UTC, task);
            final Instant oneHourAfterEpoch = Instant.EPOCH.plus(Duration.ofHours(1));
            // Gives opportunity to the EPOCH run time to be fired
            Thread.sleep(1_000);
            clock.currentTime = oneHourAfterEpoch;
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(f::isDone);

            assertThat(task.runTimes).containsExactly(
                    Instant.EPOCH.toEpochMilli(), oneHourAfterEpoch.toEpochMilli());
        }
    }

    @Test
    public void testScheduleAtRoundTimesInDayDstSkippingToLatest() throws Exception {
        final Instant day = Instant.from(of(LocalDateTime.of(2019, 4, 7, 0, 0), LORD_HOWE_TZ));
        final Instant nextDay = Instant.from(of(LocalDateTime.of(2019, 4, 8, 0, 0), LORD_HOWE_TZ));
        final SettableClock clock = new SettableClock(day);
        final CronScheduler s = CronScheduler.newBuilder(ONE_SEC).setTimeProvider(clock).build();
        try (AutoCloseable ignore = asCloseable(s)) {
            final TimesRecordingCronTask task = new TimesRecordingCronTask(2);
            final Future<?> f = s.scheduleAtRoundTimesInDaySkippingToLatest(
                    Duration.ofSeconds(MINUTES.toSeconds(22) + 30), LORD_HOWE_TZ, task);
            // Gives opportunity to the `day` run time to be fired
            Thread.sleep(1_000);
            clock.currentTime = nextDay;
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(f::isDone);

            assertThat(task.runTimes)
                    .containsExactly(day.toEpochMilli(), nextDay.toEpochMilli());
        }
    }

    @Test
    public void testScheduleAtFixedRateClockRewind() throws Exception {
        final ZonedDateTime dt = of(LocalDateTime.of(2020, 1, 1, 0, 0), UTC);
        final SettableClock clock = new SettableClock(dt.toInstant());
        final CronScheduler s = CronScheduler.newBuilder(ONE_SEC).setTimeProvider(clock).build();
        try (AutoCloseable ignore = asCloseable(s)) {
            CountDownLatch clockRewindObserved = new CountDownLatch(1);
            CronTask task = new CronTask() {
                long lastScheduledRunTimeMillis = 0;
                @Override
                public void run(long scheduledRunTimeMillis) throws Exception {
                    if (scheduledRunTimeMillis < lastScheduledRunTimeMillis) {
                        clockRewindObserved.countDown();
                    }
                    lastScheduledRunTimeMillis = scheduledRunTimeMillis;
                }
            };
            s.scheduleAtFixedRate(0, 1, SECONDS, task);
            final Instant oneHourAfterEpoch = Instant.EPOCH.plus(Duration.ofHours(1));
            // Gives opportunity to the `dt` run time to be fired
            Thread.sleep(1_000);
            clock.currentTime = dt.minus(Duration.ofSeconds(5)).toInstant();
            clockRewindObserved.await(10, SECONDS);
        }
    }

    @Test
    public void testScheduleAtRoundTimesInDayDstClockRewind() throws Exception {
        final ZonedDateTime dt = of(LocalDateTime.of(2019, 4, 7, 2, 1), LORD_HOWE_TZ);
        final SpeedHistoricalClock clock =
                new SpeedHistoricalClock(Instant.from(dt), Duration.ofMinutes(10), ONE_SEC);
        final CronScheduler s = CronScheduler.newBuilder(ONE_SEC).setTimeProvider(clock).build();
        try (AutoCloseable ignore = asCloseable(s)) {
            final TimesRecordingCronTask task = new TimesRecordingCronTask(4);
            final Future<?> f =
                    s.scheduleAtRoundTimesInDay(Duration.ofMinutes(18), LORD_HOWE_TZ, task);
            // Gives opportunity to the `dt` run time to be fired
            Thread.sleep(1_000);
            clock.insertRewind(Duration.ofHours(1));
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(f::isDone);

            final List<ZonedDateTime> runTimes = task.getRunTimes(LORD_HOWE_TZ);
            System.out.println(runTimes);
            assertThat(runTimes.get(runTimes.size() - 1)).isBefore(runTimes.get(0));
            assertThat(runTimes.stream().map(t -> getOffset(t)).collect(Collectors.toSet()))
                    .size().isGreaterThan(1);
            assertThat(runTimes).allSatisfy(runTime -> {
                long minutesSinceStartOfDay =
                        HOURS.toMinutes(runTime.getHour()) + runTime.getMinute();
                assertThat(minutesSinceStartOfDay % 18)
                        .withFailMessage("%s is not divisible by 18 minutes", runTime)
                        .isZero();
            });
        }
    }

    private static ZoneOffset getOffset(ZonedDateTime dt) {
        return dt.getZone().getRules().getOffset(dt.toLocalDateTime());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testTasksAdd() throws Exception {
        final CronScheduler s = CronScheduler.create(ONE_SEC);
        try (AutoCloseable ignore = asCloseable(s)) {
            assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> {
                final FutureTask f = new FutureTask(() -> null);
                ((Collection) s.getTasks()).add(f);
            });
        }
    }

    @Test
    public void testPrestartThread() throws Exception {
        final CronScheduler s = CronScheduler.create(ONE_SEC);
        try (AutoCloseable ignore = asCloseable(s)) {
            assertThat(s.isThreadRunning()).isFalse();
            s.prestartThread();
            assertThat(s.isThreadRunning()).isTrue();
        }
    }
}
