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

import com.splicemachine.orc.checkpoint.FloatStreamCheckpoint;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import static com.splicemachine.orc.stream.OrcStreamUtils.readFully;
import static com.splicemachine.orc.stream.OrcStreamUtils.skipFully;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;

public class FloatStream
        implements ValueStream<FloatStreamCheckpoint>
{
    private final OrcInputStream input;
    private final byte[] buffer = new byte[SIZE_OF_FLOAT];
    private final Slice slice = Slices.wrappedBuffer(buffer);

    public FloatStream(OrcInputStream input)
    {
        this.input = input;
    }

    @Override
    public Class<FloatStreamCheckpoint> getCheckpointType()
    {
        return FloatStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(FloatStreamCheckpoint checkpoint)
            throws IOException
    {
        input.seekToCheckpoint(checkpoint.getInputStreamCheckpoint());
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        long length = items * SIZE_OF_FLOAT;
        skipFully(input, length);
    }

    public float next()
            throws IOException
    {
        readFully(input, buffer, 0, SIZE_OF_FLOAT);
        return slice.getFloat(0);
    }

    public void nextVector(DataType type, int items, ColumnVector columnVector)
            throws IOException
    {
        for (int i = 0, j = 0; i < items; i++) {
            while (columnVector.isNullAt(i+j)) {
                columnVector.appendNull();
                j++;
            }
            columnVector.appendFloat(next());
        }
    }

    public void nextVector(DataType type, long items, ColumnVector columnVector, boolean[] isNull)
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
                columnVector.appendFloat(next());
            }
        }
    }
}
