/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import com.splicemachine.orc.OrcDataSourceId;
import com.splicemachine.orc.checkpoint.StreamCheckpoint;
import com.splicemachine.orc.metadata.Stream;
import com.splicemachine.orc.metadata.Stream.StreamKind;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestValueStream<T, C extends StreamCheckpoint, W extends ValueOutputStream<C>, R extends ValueStream<C>>
{
    static final OrcDataSourceId ORC_DATA_SOURCE_ID = new OrcDataSourceId("test");

    protected void testWriteValue(List<List<T>> groups)
            throws IOException
    {
        W outputStream = createValueOutputStream();
        for (int i = 0; i < 3; i++) {
            outputStream.reset();
            long retainedBytes = 0;
            for (List<T> group : groups) {
                outputStream.recordCheckpoint();
                group.forEach(value -> writeValue(outputStream, value));

                assertTrue(outputStream.getRetainedBytes() >= retainedBytes);
                retainedBytes = outputStream.getRetainedBytes();
            }
            outputStream.close();

            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1000);
            Optional<Stream> stream = outputStream.writeDataStreams(33, sliceOutput);
            assertTrue(stream.isPresent());
            assertEquals(stream.get().getStreamKind(), StreamKind.DATA);
            assertEquals(stream.get().getColumn(), 33);
            assertEquals(stream.get().getLength(), sliceOutput.size());

            List<C> checkpoints = outputStream.getCheckpoints();
            assertEquals(checkpoints.size(), groups.size());

            R valueStream = createValueStream(sliceOutput.slice());
            for (List<T> group : groups) {
                int index = 0;
                for (T expectedValue : group) {
                    index++;
                    T actualValue = readValue(valueStream);
                    if (!actualValue.equals(expectedValue)) {
                        assertEquals(actualValue, expectedValue, "index=" + index);
                    }
                }
            }
            for (int groupIndex = groups.size() - 1; groupIndex >= 0; groupIndex--) {
                valueStream.seekToCheckpoint(checkpoints.get(groupIndex));
                for (T expectedValue : groups.get(groupIndex)) {
                    T actualValue = readValue(valueStream);
                    if (!actualValue.equals(expectedValue)) {
                        assertEquals(actualValue, expectedValue);
                    }
                }
            }
        }
    }

    protected abstract W createValueOutputStream();

    protected abstract void writeValue(W outputStream, T value);

    protected abstract R createValueStream(Slice slice)
            throws OrcCorruptionException;

    protected abstract T readValue(R valueStream)
            throws IOException;
}
