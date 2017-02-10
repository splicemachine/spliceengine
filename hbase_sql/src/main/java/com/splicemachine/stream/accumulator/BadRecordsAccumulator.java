/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
