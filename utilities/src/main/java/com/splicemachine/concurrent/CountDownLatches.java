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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Utilities for working with CountDownLatch.
 */
public class CountDownLatches {

    /**
     * If calling await in a context where the calling thread being interrupted is unexpected/programmer-error then
     * this unchecked version is more concise.
     */
    public static void uncheckedAwait(CountDownLatch countDownLatch) {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * If calling await in a context where the calling thread being interrupted is unexpected/programmer-error then
     * this unchecked version is more concise.
     */
    public static boolean uncheckedAwait(CountDownLatch countDownLatch, long timeout, TimeUnit timeUnit) {
        try {
            return countDownLatch.await(timeout, timeUnit);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }


}
