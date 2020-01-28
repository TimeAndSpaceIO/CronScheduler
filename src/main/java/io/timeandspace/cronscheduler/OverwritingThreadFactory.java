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
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ThreadFactory;

final class OverwritingThreadFactory implements ThreadFactory {
    private final ThreadFactory baseThreadFactory;
    private final @Nullable Integer priority;
    final @Nullable String name;

    OverwritingThreadFactory(ThreadFactory baseThreadFactory, @Nullable Integer priority,
            @Nullable String name) {
        this.baseThreadFactory = baseThreadFactory;
        this.priority = priority;
        this.name = name;
    }

    @Override
    public Thread newThread(@NotNull Runnable r) {
        final Thread t = baseThreadFactory.newThread(r);
        if (priority != null) {
            t.setPriority(priority);
        }
        if (name != null) {
            t.setName(name);
        }
        return t;
    }
}
