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
