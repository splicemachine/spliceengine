package com.splicemachine.derby.utils;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class AtomicSequencer implements Sequencer{
    private final AtomicLong sequence = new AtomicLong(0l);

    @Override
    public long next() throws IOException{
        return sequence.incrementAndGet();
    }

    @Override
    public void setPosition(long sequence) throws IOException{
        this.sequence.set(sequence);
    }
}
