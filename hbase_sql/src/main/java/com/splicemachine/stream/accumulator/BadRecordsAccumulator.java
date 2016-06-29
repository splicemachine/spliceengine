package com.splicemachine.stream.accumulator;

import org.apache.spark.AccumulableParam;
import com.splicemachine.derby.stream.control.BadRecordsRecorder;

/**
 *
 * Accumulator for Bad Records from bulk import.
 * <p/>
 * Uses a {@link BadRecordsRecorder} to record each bad record to a temp file. Temp files
 * will be merged {@link #addInPlace(BadRecordsRecorder, BadRecordsRecorder)}.
 *
 * @see org.apache.spark.AccumulableParam
 *
 */
public class BadRecordsAccumulator implements AccumulableParam<BadRecordsRecorder, String> {

    @Override
    public BadRecordsRecorder addAccumulator(BadRecordsRecorder r, String s) {
        r.recordBadRecord(s);
        return r;
    }

    @Override
    public BadRecordsRecorder addInPlace(BadRecordsRecorder r1, BadRecordsRecorder r2) {
        return r1.merge(r2); // r1 or r2 may have no files...
    }

    @Override
    public BadRecordsRecorder zero(BadRecordsRecorder r) {
        return r;
    }
}
