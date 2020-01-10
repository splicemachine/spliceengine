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

package com.splicemachine;

import com.lmax.disruptor.*;
import java.util.concurrent.locks.LockSupport;

/**
 *
 * Strategy that provides better write throughput in HBase.
 *
 */
public class HBaseWALBlockingWaitStrategy implements WaitStrategy {
        // 2 Millisecond Delay by default
        public static final long DEFAULT_NANO_SECOND_DELAY = 1000*1000*2;
        private final long nanoSecondDelay;

        public HBaseWALBlockingWaitStrategy() {
            this(DEFAULT_NANO_SECOND_DELAY);
        }

        public HBaseWALBlockingWaitStrategy(long nanoSecondDelay) {
            this.nanoSecondDelay = nanoSecondDelay;
        }

        @Override
        public long waitFor(final long sequence, Sequence cursor, final Sequence dependentSequence, final SequenceBarrier barrier)
        throws AlertException, InterruptedException {
            LockSupport.parkNanos(nanoSecondDelay);
            long availableSequence;
            while ((availableSequence = dependentSequence.get()) < sequence) {
                barrier.checkAlert();
            }
            return availableSequence;
        }

        @Override
        public void signalAllWhenBlocking() {
        }
}
