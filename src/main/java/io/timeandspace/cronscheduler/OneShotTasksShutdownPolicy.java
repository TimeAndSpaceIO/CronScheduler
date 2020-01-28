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
 * This policy controls whether one-shot tasks already submitted to {@link CronScheduler} when
 * {@link CronScheduler#shutdown CronScheduler.shutdown} is called should be executed or discarded.
 */
public enum OneShotTasksShutdownPolicy {
    /**
     * Provided this policy, {@link CronScheduler#shutdown CronScheduler.shutdown} will not
     * terminate the scheduler until all already submitted one-shot tasks are executed.
     */
    EXECUTE_DELAYED,

    /**
     * Provided this policy, {@link CronScheduler#shutdown CronScheduler.shutdown} will discard
     * already submitted one-shot tasks <i>whose trigger time hasn't already come</i>. The one-shot
     * tasks whose trigger time is earlier than or equal to the current time (with respect to
     * {@link CronScheduler}'s time provider) will be executed anyway.
     */
    DISCARD_DELAYED
}
