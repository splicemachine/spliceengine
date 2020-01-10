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

import com.splicemachine.orc.checkpoint.LongStreamCheckpoint;
import com.splicemachine.orc.checkpoint.LongStreamDwrfCheckpoint;
import com.splicemachine.orc.metadata.OrcType.OrcTypeKind;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import static com.splicemachine.orc.stream.LongDecode.readDwrfLong;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static java.lang.Math.toIntExact;

public class LongStreamDwrf
        implements LongStream
{
    private final OrcInputStream input;
    private final OrcTypeKind orcTypeKind;
    private final boolean signed;
    private final boolean usesVInt;

    public LongStreamDwrf(OrcInputStream input, OrcTypeKind type, boolean signed, boolean usesVInt)
    {
        this.input = input;
        this.orcTypeKind = type;
        this.signed = signed;
        this.usesVInt = usesVInt;
    }

    @Override
    public Class<LongStreamDwrfCheckpoint> getCheckpointType()
    {
        return LongStreamDwrfCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(LongStreamCheckpoint checkpoint)
            throws IOException
    {
        LongStreamDwrfCheckpoint dwrfCheckpoint = (LongStreamDwrfCheckpoint) checkpoint;
        input.seekToCheckpoint(dwrfCheckpoint.getInputStreamCheckpoint());
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        // there is no fast way to skip values
        for (long i = 0; i < items; i++) {
            next();
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
    public long next()
            throws IOException
    {
        return readDwrfLong(input, orcTypeKind, signed, usesVInt);
    }

    @Override
    public void nextIntVector(int items, int[] vector)
            throws IOException
    {
        checkPositionIndex(items, vector.length);

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
    public void nextLongVector(int items, long[] vector)
            throws IOException
    {
        checkPositionIndex(items, vector.length);

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
    public void nextLongVector(DataType type, int items, ColumnVector columnVector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            columnVector.appendLong(next());
        }
    }

    @Override
    public void nextLongVector(DataType type, int items, ColumnVector columnVector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
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
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            columnVector.appendInt((int)next());
        }
    }

    @Override
    public void nextIntVector(DataType type, int items, ColumnVector columnVector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (isNull[i]) {
                columnVector.appendNull();
            }
            else {
                columnVector.appendInt((int)next());
            }
        }
    }

    @Override
    public void nextShortVector(int items, short[] vector)
            throws IOException
    {
        checkPositionIndex(items, vector.length);

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
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            columnVector.appendShort((short)next());
        }
    }

    @Override
    public void nextShortVector(DataType type, int items, ColumnVector columnVector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (isNull[i]) {
                columnVector.appendNull();
            }
            else {
                columnVector.appendShort((short)next());
            }
        }
    }
}
