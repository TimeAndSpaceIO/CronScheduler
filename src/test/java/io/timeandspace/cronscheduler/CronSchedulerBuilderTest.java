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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

import static io.timeandspace.cronscheduler.ScheduledExecutorTest.ONE_SEC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class CronSchedulerBuilderTest extends JSR166TestCase {

    public void testSetThreadFactory() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("testName").build();
        final CronScheduler s = CronScheduler.newBuilder(ONE_SEC)
                .setThreadFactory(threadFactory).build();
        try (JSR166TestCase.PoolCleaner cleaner = cleaner(s)) {
            s.prestartThread();
            assertThat(s.toString()).contains("testName");
        }
    }

    public void testSetThreadFactoryNull() {
        assertThatNullPointerException().isThrownBy(
                () -> CronScheduler.newBuilder(ONE_SEC).setThreadFactory(null).build());
    }
}
