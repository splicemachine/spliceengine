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

package com.splicemachine;

import com.lmax.disruptor.*;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by jleach on 9/28/15.
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
