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
import com.splicemachine.orc.checkpoint.LongStreamCheckpoint;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
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
import static com.splicemachine.orc.metadata.Stream.StreamKind.DATA;

public class TestLongStreamV1
        extends AbstractTestValueStream<Long, LongStreamCheckpoint, LongOutputStreamV1, LongStreamV1>
{
    @Test
    public void test()
            throws IOException
    {
        List<List<Long>> groups = new ArrayList<>();
        List<Long> group;

        group = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            group.add((long) (i));
        }
        groups.add(group);

        group = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            group.add((long) (10_000 + (i * 17)));
        }
        groups.add(group);

        group = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            group.add((long) (10_000 - (i * 17)));
        }
        groups.add(group);

        group = new ArrayList<>();
        Random random = new Random(22);
        for (int i = 0; i < 1000; i++) {
            group.add(-1000L + random.nextInt(17));
        }
        groups.add(group);

        testWriteValue(groups);
    }

    @Override
    protected LongOutputStreamV1 createValueOutputStream()
    {
        return new LongOutputStreamV1(SNAPPY, DEFAULT_BUFFER_SIZE, true, DATA);
    }

    @Override
    protected void writeValue(LongOutputStreamV1 outputStream, Long value)
    {
        outputStream.writeLong(value);
    }

    @Override
    protected LongStreamV1 createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, DEFAULT_BUFFER_SIZE);
        return new LongStreamV1(new OrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, new AggregatedMemoryContext()), true);
    }

    @Override
    protected Long readValue(LongStreamV1 valueStream)
            throws IOException
    {
        return valueStream.next();
    }
}
