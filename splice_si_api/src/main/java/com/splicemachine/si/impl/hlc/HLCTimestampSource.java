package com.splicemachine.si.impl.hlc;

import com.splicemachine.timestamp.api.TimestampSource;

/**
 * Created by jleach on 4/21/16.
 */
public class HLCTimestampSource implements TimestampSource {
    public static HLC hlc = new HLC();

    public HLCTimestampSource() {
    }


    @Override
    public long nextTimestamp() {
        return hlc.sendOrLocalEvent();
    }

    @Override
    public void rememberTimestamp(long timestamp) {
        // No Op
    }

    @Override
    public long retrieveTimestamp() {
        return 0;
    }

    @Override
    public void shutdown() {

    }
}
