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
import com.splicemachine.orc.checkpoint.DecimalStreamCheckpoint;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.types.Decimals;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static com.splicemachine.orc.OrcDecompressor.createOrcDecompressor;
import static com.splicemachine.orc.OrcWriter.DEFAULT_BUFFER_SIZE;
import static com.splicemachine.orc.metadata.CompressionKind.SNAPPY;

public class TestShortDecimalStream
        extends AbstractTestValueStream<Long, DecimalStreamCheckpoint, DecimalOutputStream, DecimalStream>
{
    @Test
    public void test()
            throws IOException
    {
        Random random = new Random(0);
        List<List<Long>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Long> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                long value = random.nextLong();
                group.add(value);
            }
            groups.add(group);
        }
        testWriteValue(groups);
    }

    @Override
    protected DecimalOutputStream createValueOutputStream()
    {
        return new DecimalOutputStream(SNAPPY, DEFAULT_BUFFER_SIZE);
    }

    @Override
    protected void writeValue(DecimalOutputStream outputStream, Long value)
    {
        outputStream.writeUnscaledValue(Decimals.encodeUnscaledValue(value));
    }

    @Override
    protected DecimalStream createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, DEFAULT_BUFFER_SIZE);
        return new DecimalStream(new OrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, new AggregatedMemoryContext()));
    }

    @Override
    protected Long readValue(DecimalStream valueStream)
            throws IOException
    {
        return valueStream.nextLong();
    }
}
