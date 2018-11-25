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

import com.splicemachine.orc.OrcCorruptionException;
import com.splicemachine.orc.OrcDecompressor;
import com.splicemachine.orc.checkpoint.BooleanStreamCheckpoint;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.metadata.Stream;
import com.splicemachine.orc.metadata.Stream.StreamKind;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.splicemachine.orc.OrcDecompressor.createOrcDecompressor;
import static com.splicemachine.orc.OrcWriter.DEFAULT_BUFFER_SIZE;
import static com.splicemachine.orc.metadata.CompressionKind.SNAPPY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestBooleanStream
        extends AbstractTestValueStream<Boolean, BooleanStreamCheckpoint, BooleanOutputStream, BooleanStream>
{
    @Test
    public void test()
            throws IOException
    {
        List<List<Boolean>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Boolean> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add(i % 3 == 0);
            }
            groups.add(group);
        }
        List<Boolean> group = new ArrayList<>();
        for (int i = 0; i < 17; i++) {
            group.add(i % 3 == 0);
        }
        groups.add(group);
        testWriteValue(groups);
    }

    @Test
    public void testWriteMultiple()
            throws IOException
    {
        BooleanOutputStream outputStream = createValueOutputStream();
        for (int i = 0; i < 3; i++) {
            outputStream.reset();

            BooleanList expectedValues = new BooleanArrayList(1024);
            outputStream.writeBooleans(32, true);
            expectedValues.addAll(Collections.nCopies(32, true));
            outputStream.writeBooleans(32, false);
            expectedValues.addAll(Collections.nCopies(32, false));

            outputStream.writeBooleans(1, true);
            expectedValues.add(true);
            outputStream.writeBooleans(1, false);
            expectedValues.add(false);

            outputStream.writeBooleans(34, true);
            expectedValues.addAll(Collections.nCopies(34, true));
            outputStream.writeBooleans(34, false);
            expectedValues.addAll(Collections.nCopies(34, false));

            outputStream.writeBoolean(true);
            expectedValues.add(true);
            outputStream.writeBoolean(false);
            expectedValues.add(false);

            outputStream.close();

            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1000);
            Optional<Stream> stream = outputStream.writeDataStreams(33, sliceOutput);
            assertTrue(stream.isPresent());
            assertEquals(stream.get().getStreamKind(), StreamKind.DATA);
            assertEquals(stream.get().getColumn(), 33);
            assertEquals(stream.get().getLength(), sliceOutput.size());

            BooleanStream valueStream = createValueStream(sliceOutput.slice());
            for (int index = 0; index < expectedValues.size(); index++) {
                boolean expectedValue = expectedValues.getBoolean(index);
                boolean actualValue = readValue(valueStream);
                assertEquals(actualValue, expectedValue);
            }
        }
    }

    @Override
    protected BooleanOutputStream createValueOutputStream()
    {
        return new BooleanOutputStream(SNAPPY, DEFAULT_BUFFER_SIZE);
    }

    @Override
    protected void writeValue(BooleanOutputStream outputStream, Boolean value)
    {
        outputStream.writeBoolean(value);
    }

    @Override
    protected BooleanStream createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, DEFAULT_BUFFER_SIZE);
        return new BooleanStream(new OrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, new AggregatedMemoryContext()));
    }

    @Override
    protected Boolean readValue(BooleanStream valueStream)
            throws IOException
    {
        return valueStream.nextBit();
    }
}
