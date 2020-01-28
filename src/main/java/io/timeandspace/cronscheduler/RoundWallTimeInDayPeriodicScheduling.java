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
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;

import static io.timeandspace.cronscheduler.MathUtils.roundDown;
import static io.timeandspace.cronscheduler.MathUtils.roundUp;
import static java.util.concurrent.TimeUnit.*;

/**
 * TODO add caching of {@link ZoneRules} for dynamically loaded zones on the library level
 */
class RoundWallTimeInDayPeriodicScheduling<V> extends PeriodicScheduling<V> {

    private final ZoneId zoneId;
    private final long periodSeconds;
    private final boolean skippingToLatest;

    private long nextScheduledRunTimeSeconds;

    RoundWallTimeInDayPeriodicScheduling(Clock timeProvider, CronTask task, ZoneId zoneId,
            long periodSeconds, boolean skippingToLatest) {
        super(timeProvider, task);
        this.zoneId = zoneId;
        this.periodSeconds = periodSeconds;
        this.skippingToLatest = skippingToLatest;
        final Instant currentTime = timeProvider.instant();
        long definitelySchedulableRunTimeSeconds =
                rewindToValidMidnight(ZonedDateTime.ofInstant(currentTime, zoneId)).toEpochSecond();
        final long currentTimeSeconds = currentTime.getEpochSecond();
        final ZoneRules zoneRules = zoneId.getRules();
        long firstScheduledRunTimeSeconds = definitelySchedulableRunTimeSeconds;
        while (firstScheduledRunTimeSeconds < currentTimeSeconds) {
            firstScheduledRunTimeSeconds = computeNextRunTimeSeconds(zoneRules, periodSeconds,
                    firstScheduledRunTimeSeconds);
        }
        nextScheduledRunTimeSeconds = firstScheduledRunTimeSeconds;
    }

    /**
     * See the documentation for {@link LocalDate#atStartOfDay(ZoneId)}; if an offset transition
     * happens across a day boundary, midnight of a specific day may not be a valid date-time.
     */
    private static ZonedDateTime rewindToValidMidnight(ZonedDateTime zonedDateTime) {
        final ZoneId zone = zonedDateTime.getZone();
        for (LocalDate localDate = zonedDateTime.toLocalDate();;
             localDate = localDate.minusDays(1)) {
            ZonedDateTime maybeMidnight = localDate.atStartOfDay(zone);
            if (maybeMidnight.getHour() == 0 && maybeMidnight.getMinute() == 0) {
                return maybeMidnight;
            }
        }
    }

    @Override
    public V call() throws Exception {
        if (skippingToLatest) {
            final Instant currentTime = timeProvider.instant();
            final long currentTimeSeconds = currentTime.getEpochSecond();
            final ZoneRules zoneRules = zoneId.getRules();
            long candidateLatestScheduledRunTimeSeconds;
            while ((candidateLatestScheduledRunTimeSeconds = computeNextRunTimeSeconds(zoneRules,
                    periodSeconds, nextScheduledRunTimeSeconds)) <= currentTimeSeconds) {
                nextScheduledRunTimeSeconds = candidateLatestScheduledRunTimeSeconds;
            }
        }
        task.run(nextScheduledRunTimeMillis());
        return null;
    }

    @Override
    public void setNextRunTime() {
        nextScheduledRunTimeSeconds = computeNextRunTimeSeconds(zoneId.getRules(),
                periodSeconds, nextScheduledRunTimeSeconds);
    }

    private static long computeNextRunTimeSeconds(
            ZoneRules zoneRules, long periodSeconds, long prevRunTime) {
        final ZoneOffsetTransition nextTransition =
                zoneRules.nextTransition(Instant.ofEpochSecond(prevRunTime));
        long nextOrdinaryScheduledRunTimeSeconds = prevRunTime + periodSeconds;
        if (nextTransition == null ||
                // Using >, not >= because the moment of transition is *not* represented by the
                // local time 'before': see the doc for ZoneOffsetTransition.getDateTimeBefore.
                nextTransition.getInstant().getEpochSecond() >
                        nextOrdinaryScheduledRunTimeSeconds) {
            return nextOrdinaryScheduledRunTimeSeconds;
        }
        // Same logic for gap and overlap transitions
        final int secondsSinceStartOfDayAfterTransition =
                getSecondsSinceStartOfDay(nextTransition.getDateTimeAfter());
        // There is no cross-day concerns, even if in some imaginary TZ a transition happens across
        // the day boundary, because 00:00 is always a round wall time for CronScheduler.*InDay*
        // methods.
        final long nextRoundTimeSecondsInDay =
                roundUp(secondsSinceStartOfDayAfterTransition, periodSeconds);
        final long schedulingDelayAfterTransitionSeconds =
                nextRoundTimeSecondsInDay - secondsSinceStartOfDayAfterTransition;
        return nextTransition.getInstant().getEpochSecond() + schedulingDelayAfterTransitionSeconds;
    }

    private static long computePrevRunTimeSeconds(
            ZoneRules zoneRules, long periodSeconds, long nextRunTime) {
        final ZoneOffsetTransition prevTransition =
                // If nextRunTime is specifically the moment of transition, we want to get *this*
                // transition as prevTransition, not the one 6 months earlier. For this reason,
                // adding one second.
                zoneRules.previousTransition(Instant.ofEpochSecond(nextRunTime + 1));
        long prevOrdinaryScheduledRunTimeSeconds = nextRunTime - periodSeconds;
        if (prevTransition == null ||
                // Using <=, not < because the moment of transition is represented by the local time
                // 'after': see the doc for ZoneOffsetTransition.getDateTimeAfter.
                prevTransition.getInstant().getEpochSecond() <=
                        prevOrdinaryScheduledRunTimeSeconds) {
            return prevOrdinaryScheduledRunTimeSeconds;
        }
        // Same logic for gap and overlap transitions
        final int secondsSinceStartOfDayBeforeTransition =
                getSecondsSinceStartOfDay(prevTransition.getDateTimeBefore());
        // There is no cross-day concerns, even if in some imaginary TZ a transition happens across
        // the day boundary, because 00:00 is always a round wall time in CronScheduler.*InDay*
        // methods.
        long prevRoundTimeSecondsInDay =
                roundDown(secondsSinceStartOfDayBeforeTransition, periodSeconds);
        if (prevRoundTimeSecondsInDay == secondsSinceStartOfDayBeforeTransition) {
            // prevTransition.getDateTimeBefore() is not a valid local time, so it couldn't be a
            // scheduled time. This operation may result in prevRoundTimeSecondsInDay becoming
            // negative (like going to the previous day), which is fine since this variable is only
            // used in a computation of a duration below.
            prevRoundTimeSecondsInDay -= periodSeconds;
        }
        final long durationSincePrevRoundTimeBeforeTransitionSeconds =
                secondsSinceStartOfDayBeforeTransition - prevRoundTimeSecondsInDay;
        return prevTransition.getInstant().getEpochSecond() -
                durationSincePrevRoundTimeBeforeTransitionSeconds;
    }

    private static int getSecondsSinceStartOfDay(LocalDateTime dt) {
        return Math.toIntExact(MINUTES.toSeconds(HOURS.toMinutes(dt.getHour()) + dt.getMinute()) +
                dt.getSecond());
    }

    @Override
    public boolean rewind(long newTimeMillis) {
        if (nextScheduledRunTimeMillis() <= newTimeMillis) {
            return false;
        }
        final ZoneRules zoneRules = zoneId.getRules();
        while (nextScheduledRunTimeMillis() > newTimeMillis) {
            this.nextScheduledRunTimeSeconds = computePrevRunTimeSeconds(zoneRules,
                    periodSeconds, nextScheduledRunTimeSeconds);
        }
        return true;
    }

    @Override
    public long nextScheduledRunTimeMillis() {
        return SECONDS.toMillis(nextScheduledRunTimeSeconds);
    }
}
