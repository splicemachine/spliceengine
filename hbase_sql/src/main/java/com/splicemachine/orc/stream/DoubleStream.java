/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splicemachine.orc.stream;

import com.splicemachine.orc.checkpoint.DoubleStreamCheckpoint;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import java.io.IOException;
import static com.splicemachine.orc.stream.OrcStreamUtils.readFully;
import static com.splicemachine.orc.stream.OrcStreamUtils.skipFully;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public class DoubleStream
        implements ValueStream<DoubleStreamCheckpoint>
{
    private final OrcInputStream input;
    private final byte[] buffer = new byte[SIZE_OF_DOUBLE];
    private final Slice slice = Slices.wrappedBuffer(buffer);

    public DoubleStream(OrcInputStream input)
    {
        this.input = input;
    }

    @Override
    public Class<DoubleStreamCheckpoint> getCheckpointType()
    {
       return DoubleStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(DoubleStreamCheckpoint checkpoint)
            throws IOException
    {
        input.seekToCheckpoint(checkpoint.getInputStreamCheckpoint());
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        long length = items * SIZE_OF_DOUBLE;
        skipFully(input, length);
    }

    public double next()
            throws IOException
    {
        readFully(input, buffer, 0, SIZE_OF_DOUBLE);
        return slice.getDouble(0);
    }

    public void nextVector(DataType type, int items, ColumnVector columnVector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            columnVector.appendDouble(next());
        }
    }

    public void nextVector(DataType type, long items, ColumnVector columnVector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (isNull[i]) {
                columnVector.appendNull();
            }
            else {
                columnVector.appendDouble(next());
            }
        }
    }
}
