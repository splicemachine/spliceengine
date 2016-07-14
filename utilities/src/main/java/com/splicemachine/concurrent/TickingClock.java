/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.concurrent;

/**
 * @author Scott Fines
 *         Date: 8/14/15
 */
public interface TickingClock extends Clock{

    /**
     * Move forward the clock by {@code millis} milliseconds.
     *
     * @param millis the millisecond to move forward by
     * @return the value of the clock (in milliseconds) after ticking forward
     */
    long tickMillis(long millis);

    /**
     * Move forward the clock by {@code nanos} nanoseconds.
     *
     * @param nanos the nanos to move forward by
     * @return the value of the clock (in nanoseconds) after moving forward.
     */
    long tickNanos(long nanos);
}
