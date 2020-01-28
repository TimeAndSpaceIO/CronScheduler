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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

final class DefaultCronSchedulerThreadFactory implements ThreadFactory {
    private static final AtomicLong NAME_SEQUENCE_NUMBER = new AtomicLong();

    static DefaultCronSchedulerThreadFactory INSTANCE =
            new DefaultCronSchedulerThreadFactory();

    private DefaultCronSchedulerThreadFactory() {}

    @Override
    public Thread newThread(@NotNull Runnable r) {
        final Thread t = new Thread(r);
        t.setName("cron-scheduler-" + NAME_SEQUENCE_NUMBER.incrementAndGet());
        t.setDaemon(true);
        return t;
    }
}
