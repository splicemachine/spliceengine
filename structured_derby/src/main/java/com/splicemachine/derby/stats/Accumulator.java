package com.splicemachine.derby.stats;

/**
 * Statistical Data accumulator for gathering up statistics during a run.
 *
 * @author Scott Fines
 * Created on: 2/26/13
 */
public interface Accumulator {

    void start();

    Stats finish();

    void tick(long numRecords, long timeTaken);

    void tick(long timeTaken);
}
