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
