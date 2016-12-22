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
    public long[] nextTimestamps(int batch) {
        long[] timestamps = new long[batch];
        for (int i =0; i< batch;i++)
            timestamps[i] = nextTimestamp();
        return timestamps;
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
