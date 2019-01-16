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
import com.splicemachine.orc.checkpoint.ByteStreamCheckpoint;
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

public class TestByteStream
        extends AbstractTestValueStream<Byte, ByteStreamCheckpoint, ByteOutputStream, ByteStream>
{
    @Test
    public void testLiteral()
            throws IOException
    {
        List<List<Byte>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Byte> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add((byte) (groupIndex * 10_000 + i));
            }
            groups.add(group);
        }
        testWriteValue(groups);
    }

    @Test
    public void testRleLong()
            throws IOException
    {
        List<List<Byte>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Byte> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add((byte) 77);
            }
            groups.add(group);
        }
        testWriteValue(groups);
    }

    @Test
    public void testRleShort()
            throws IOException
    {
        List<List<Byte>> groups = new ArrayList<>();
        for (int groupIndex = 0; groupIndex < 3; groupIndex++) {
            List<Byte> group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add((byte) ((groupIndex * 10_000 + i) / 13));
            }
            groups.add(group);
        }
        testWriteValue(groups);
    }

    @Override
    protected ByteOutputStream createValueOutputStream()
    {
        return new ByteOutputStream(SNAPPY, DEFAULT_BUFFER_SIZE);
    }

    @Override
    protected void writeValue(ByteOutputStream outputStream, Byte value)
    {
        outputStream.writeByte(value);
    }

    @Override
    protected ByteStream createValueStream(Slice slice)
            throws OrcCorruptionException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, DEFAULT_BUFFER_SIZE);
        return new ByteStream(new OrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, new AggregatedMemoryContext()));
    }

    @Override
    protected Byte readValue(ByteStream valueStream)
            throws IOException
    {
        return valueStream.next();
    }
}
