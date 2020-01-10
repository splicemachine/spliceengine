/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
