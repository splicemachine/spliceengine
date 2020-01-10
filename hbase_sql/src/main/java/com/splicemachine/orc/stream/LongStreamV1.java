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
package com.splicemachine.orc.stream;

import com.splicemachine.orc.OrcCorruptionException;
import com.splicemachine.orc.checkpoint.LongStreamCheckpoint;
import com.splicemachine.orc.checkpoint.LongStreamV1Checkpoint;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import static com.splicemachine.orc.stream.OrcStreamUtils.MIN_REPEAT_SIZE;
import static java.lang.Math.toIntExact;

public class LongStreamV1
        implements LongStream
{
    private static final int MAX_LITERAL_SIZE = 128;

    private final OrcInputStream input;
    private final boolean signed;
    private final long[] literals = new long[MAX_LITERAL_SIZE];
    private int numLiterals;
    private int delta;
    private int used;
    private boolean repeat;
    private long lastReadInputCheckpoint;

    public LongStreamV1(OrcInputStream input, boolean signed)
    {
        this.input = input;
        this.signed = signed;
        lastReadInputCheckpoint = input.getCheckpoint();
    }

    // This comes from the Apache Hive ORC code
    private void readValues()
            throws IOException
    {
        lastReadInputCheckpoint = input.getCheckpoint();

        int control = input.read();
        if (control == -1) {
            throw new OrcCorruptionException("Read past end of RLE integer from %s", input);
        }

        if (control < 0x80) {
            numLiterals = control + MIN_REPEAT_SIZE;
            used = 0;
            repeat = true;
            delta = input.read();
            if (delta == -1) {
                throw new OrcCorruptionException("End of stream in RLE Integer from %s", input);
            }

            // convert from 0 to 255 to -128 to 127 by converting to a signed byte
            // noinspection SillyAssignment
            delta = (byte) delta;
            literals[0] = LongDecode.readVInt(signed, input);
        }
        else {
            numLiterals = 0x100 - control;
            used = 0;
            repeat = false;
            for (int i = 0; i < numLiterals; ++i) {
                literals[i] = LongDecode.readVInt(signed, input);
            }
        }
    }

    @Override
    // This comes from the Apache Hive ORC code
    public long next()
            throws IOException
    {
        long result;
        if (used == numLiterals) {
            readValues();
        }
        if (repeat) {
            result = literals[0] + (used++) * delta;
        }
        else {
            result = literals[used++];
        }
        return result;
    }

    @Override
    public Class<? extends LongStreamV1Checkpoint> getCheckpointType()
    {
        return LongStreamV1Checkpoint.class;
    }

    @Override
    public void seekToCheckpoint(LongStreamCheckpoint checkpoint)
            throws IOException
    {
        LongStreamV1Checkpoint v1Checkpoint = (LongStreamV1Checkpoint) checkpoint;

        // if the checkpoint is within the current buffer, just adjust the pointer
        if (lastReadInputCheckpoint == v1Checkpoint.getInputStreamCheckpoint() && v1Checkpoint.getOffset() <= numLiterals) {
            used = v1Checkpoint.getOffset();
        }
        else {
            // otherwise, discard the buffer and start over
            input.seekToCheckpoint(v1Checkpoint.getInputStreamCheckpoint());
            numLiterals = 0;
            used = 0;
            skip(v1Checkpoint.getOffset());
        }
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        while (items > 0) {
            if (used == numLiterals) {
                readValues();
            }
            long consume = Math.min(items, numLiterals - used);
            used += consume;
            items -= consume;
        }
    }

    @Override
    public long sum(int items)
            throws IOException
    {
        long sum = 0;
        for (int i = 0; i < items; i++) {
            sum += next();
        }
        return sum;
    }

    @Override
    public void nextLongVector(DataType type, int items, ColumnVector columnVector)
            throws IOException
    {
        for (int i = 0, j = 0; i < items; i++) {
            while (columnVector.isNullAt(i+j)) {
                columnVector.appendNull();
                j++;
            }
            columnVector.appendLong(next());
        }
    }

    @Override
    public void nextLongVector(DataType type, int items, ColumnVector columnVector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0, j = 0; i < items; i++) {
            while (columnVector.isNullAt(i+j)) {
                columnVector.appendNull();
                j++;
            }
            if (isNull[i]) {
                columnVector.appendNull();
            }
            else {
                columnVector.appendLong(next());
            }
        }
    }

    @Override
    public void nextIntVector(DataType type, int items, ColumnVector columnVector)
            throws IOException {
        for (int i = 0, j = 0; i < items; i++) {
            while (columnVector.isNullAt(i+j)) {
                columnVector.appendNull();
                j++;
            }
            columnVector.appendInt((int)next());
        }
    }

    @Override
    public void nextIntVector(DataType type, int items, ColumnVector columnVector, boolean[] isNull)
            throws IOException {
        for (int i = 0, j = 0; i < items; i++) {
            while (columnVector.isNullAt(i+j)) {
                columnVector.appendNull();
                j++;
            }
            if (isNull[i]) {
                columnVector.appendNull();
            }
            else {
                columnVector.appendInt((int)next());
            }
        }
    }

    @Override
    public void nextLongVector(int items, long[] vector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            vector[i] = next();
        }
    }

    @Override
    public void nextLongVector(int items, long[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = next();
            }
        }
    }

    @Override
    public void nextIntVector(int items, int[] vector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            vector[i] = toIntExact(next());
        }
    }

    @Override
    public void nextIntVector(int items, int[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = toIntExact(next());
            }
        }
    }

    @Override
    public void nextShortVector(int items, short[] vector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            vector[i] = (short)next();
        }
    }

    @Override
    public void nextShortVector(int items, short[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = (short)next();
            }
        }
    }

    @Override
    public void nextShortVector(DataType type, int items, ColumnVector columnVector)
            throws IOException {
        for (int i = 0, j = 0; i < items; i++) {
            while (columnVector.isNullAt(i+j)) {
                columnVector.appendNull();
                j++;
            }
            columnVector.appendShort((short)next());
        }
    }

    @Override
    public void nextShortVector(DataType type, int items, ColumnVector columnVector, boolean[] isNull)
            throws IOException {
        for (int i = 0, j = 0; i < items; i++) {
            while (columnVector.isNullAt(i+j)) {
                columnVector.appendNull();
                j++;
            }
            if (isNull[i]) {
                columnVector.appendNull();
            }
            else {
                columnVector.appendShort((short)next());
            }
        }
    }
}
