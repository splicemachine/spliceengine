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

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CountDownLatchesTest {

    @Test
    public void testUncheckedAwait() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                latch.countDown();
            }
        }.start();

        CountDownLatches.uncheckedAwait(latch);
    }

    @Test
    public void testUncheckedAwaitWithTimeout() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                latch.countDown();
            }
        }.start();

        assertTrue(CountDownLatches.uncheckedAwait(latch, 5, TimeUnit.SECONDS));
    }

    @Test
    public void testUncheckedAwaitWithTimeoutReturnsFalse() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                Threads.sleep(5, TimeUnit.SECONDS);
            }
        }.start();

        assertFalse(CountDownLatches.uncheckedAwait(latch, 50, TimeUnit.MILLISECONDS));
    }

}
