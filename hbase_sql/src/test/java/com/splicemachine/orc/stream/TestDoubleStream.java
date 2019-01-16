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
