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

/**
 * An interface for periodic tasks run by {@link CronScheduler}.
 */
@FunctionalInterface
public interface CronTask {
    /**
     * Runs the periodic task.
     *
     * <p>Periodic calls to this method from {@link CronScheduler} are <i>not</i> guaranteed to
     * progress in {@code scheduledRunTimeMillis} in the face of backward time shifts. See the
     * class-level documentation of {@link CronScheduler} for more details.
     *
     * @param scheduledRunTimeMillis the nominal time in milliseconds when this method should have
     *        been run.
     * @throws Exception if unable to run the task
     */
    void run(long scheduledRunTimeMillis) throws Exception;
}
