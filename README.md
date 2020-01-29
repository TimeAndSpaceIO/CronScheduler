# CronScheduler: a reliable Java scheduler for external interactions

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.timeandspace/cron-scheduler/badge.svg)](
https://maven-badges.herokuapp.com/maven-central/io.timeandspace/cron-scheduler)
[![Build Status](https://travis-ci.org/TimeAndSpaceIO/CronScheduler.svg?branch=master)](
https://travis-ci.org/TimeAndSpaceIO/CronScheduler)

`CronScheduler` is an alternative to `ScheduledThreadPoolExecutor` and `Timer` with the following
advantages:

 - CronScheduler is proof against unbounded [clock drift](https://en.wikipedia.org/wiki/Clock_drift)
 relative to UTC or system time for both one-shot or periodic tasks.
 - It [takes into account machine suspension](https://bugs.openjdk.java.net/browse/JDK-8146527)
 (like sleep or hibernation).
 - It has convenient methods to prevent several tasks from piling up in case of blocks, long GC
 pauses, or abrupt forward system time shifts (likely made by the user or the administrator of the
 machine).
 - It correctly handles significant system time setbacks (backward shifts), likely made by the user
 or the administrator of the machine.
 - It has convenient methods to schedule tasks at round times within a day in a given time zone
 (for example, every midnight), transparently handling all time zone and [daylight saving time](
 https://en.wikipedia.org/wiki/Daylight_saving_time) complexity.

See [this blog post](
https://medium.com/@leventov/cronscheduler-a-reliable-java-scheduler-for-external-interactions-cb7ce4a4f2cd)
for more details and specific recommendations about when to use `ScheduledThreadPoolExecutor`,
`CronScheduler`, or other scheduling facilities.

## Usage

Maven:
```xml
<dependency>
  <groupId>io.timeandspace</groupId>
  <artifactId>cron-scheduler</artifactId>
  <version>0.1</version>
</dependency>
```

Gradle:
```xml
dependencies {
  compile 'io.timeandspace:cron-scheduler:0.1'
}
```

Server-side usage example:
```java
Duration syncPeriod = Duration.ofMinutes(1);
CronScheduler cron = CronScheduler.create(syncPeriod);
cron.scheduleAtFixedRateSkippingToLatest(0, 1, TimeUnit.MINUTES, runTimeMillis -> {
    // Collect and send summary metrics to a remote monitoring system
});
```

Client-side usage example:
```java
Duration oneHour = Duration.ofHours(1);
CronScheduler cron = CronScheduler.create(oneHour);
cron.scheduleAtRoundTimesInDaySkippingToLatest(oneHour, ZoneId.systemDefault(), runTimeMillis -> {
    notifyUser("It's time to get up and make a short break from work!");
});
```

For drop-in replacement of `ScheduledThreadPoolExecutor` and integration with existing code, there
is an adapter: `cronScheduler.asScheduledExecutorService()`.

See [**Javadocs**](
https://javadoc.io/doc/io.timeandspace/cron-scheduler/latest/io/timeandspace/cronscheduler/CronScheduler.html).
