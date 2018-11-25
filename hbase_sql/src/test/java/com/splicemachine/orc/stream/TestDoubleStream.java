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
import com.splicemachine.orc.checkpoint.DoubleStreamCheckpoint;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import static com.splicemachine.orc.OrcDecompressor.createOrcDecompressor;
import static com.splicemachine.orc.OrcWriter.DEFAULT_BUFFER_SIZE;
import static com.splicemachine.orc.metadata.CompressionKind.SNAPPY;

public class TestDoubleStream
        extends AbstractTestValueStream<Double, DoubleStreamCheckpoint, DoubleOutputStream, DoubleStream>
{
    @Test
    public void test()
            throws IOException
    {
        List<List<Double>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Double> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add((double) (groupIndex * 10_000 + i));
            }
            groups.add(group);
        }
        testWriteValue(groups);
    }

    @Override
    protected DoubleOutputStream createValueOutputStream()
    {
        return new DoubleOutputStream(SNAPPY, DEFAULT_BUFFER_SIZE);
    }

    @Override
    protected void writeValue(DoubleOutputStream outputStream, Double value)
    {
        outputStream.writeDouble(value);
    }

    @Override
    protected DoubleStream createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, DEFAULT_BUFFER_SIZE);
        return new DoubleStream(new OrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, new AggregatedMemoryContext()));
    }

    @Override
    protected Double readValue(DoubleStream valueStream)
            throws IOException
    {
        return valueStream.next();
    }
}
