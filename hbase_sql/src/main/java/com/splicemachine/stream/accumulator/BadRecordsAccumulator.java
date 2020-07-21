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

package com.splicemachine.stream.accumulator;

import com.splicemachine.derby.stream.control.BadRecordsRecorder;
import org.apache.spark.util.AccumulatorV2;

/**
 *
 * Accumulator for Bad Records from bulk import.
 * <p/>
 * Uses a {@link BadRecordsRecorder} to record each bad record to a temp file. Temp files
 * will be merged {@link #merge(AccumulatorV2<String, BadRecordsRecorder>)}.
 *
 * @see org.apache.spark.util.AccumulatorV2;
 *
 */
public class BadRecordsAccumulator extends AccumulatorV2<String, BadRecordsRecorder> {

    private final BadRecordsRecorder badRecordsRecorder;

    public BadRecordsAccumulator(BadRecordsRecorder badRecordsRecorder) {
        this.badRecordsRecorder = badRecordsRecorder;
    }

    @Override
    public boolean isZero() {
        return badRecordsRecorder.getNumberOfBadRecords() == 0;
    }

    @Override
    public AccumulatorV2<String, BadRecordsRecorder> copy() {
        BadRecordsRecorder copyBadRecordsRecorder = new BadRecordsRecorder(
                badRecordsRecorder.getStatusDirectory(),
                badRecordsRecorder.getInputFilePath(),
                badRecordsRecorder.getBadRecordTolerance());

        return new BadRecordsAccumulator(copyBadRecordsRecorder);
    }

    @Override
    public void reset() {
        badRecordsRecorder.reset();
    }

    @Override
    public void add(String badRecord) {
        badRecordsRecorder.recordBadRecord(badRecord);
    }

    @Override
    public void merge(AccumulatorV2<String, BadRecordsRecorder> other) {
        badRecordsRecorder.merge(other.value());
    }

    @Override
    public BadRecordsRecorder value() {
        return badRecordsRecorder;
    }
}
