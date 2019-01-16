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
package com.splicemachine.orc.metadata;

import com.splicemachine.orc.OrcOutputBuffer;
import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.util.List;

public class CompressedMetadataWriter
        implements MetadataWriter
{
    private final MetadataWriter metadataWriter;
    private final OrcOutputBuffer buffer;

    public CompressedMetadataWriter(MetadataWriter metadataWriter, CompressionKind compression, int bufferSize)
    {
        this.metadataWriter = metadataWriter;
        this.buffer = new OrcOutputBuffer(compression, bufferSize);
    }

    @Override
    public List<Integer> getOrcMetadataVersion()
    {
        return metadataWriter.getOrcMetadataVersion();
    }

    @Override
    public int writePostscript(SliceOutput output, int footerLength, int metadataLength, CompressionKind compression, int compressionBlockSize)
            throws IOException
    {
        // postscript is not compressed
        return metadataWriter.writePostscript(output, footerLength, metadataLength, compression, compressionBlockSize);
    }

    @Override
    public int writeMetadata(SliceOutput output, Metadata metadata)
            throws IOException
    {
        buffer.reset();
        metadataWriter.writeMetadata(buffer, metadata);
        return buffer.writeDataTo(output);
    }

    @Override
    public int writeFooter(SliceOutput output, Footer footer)
            throws IOException
    {
        buffer.reset();
        metadataWriter.writeFooter(buffer, footer);
        return buffer.writeDataTo(output);
    }

    @Override
    public int writeStripeFooter(SliceOutput output, StripeFooter footer)
            throws IOException
    {
        buffer.reset();
        metadataWriter.writeStripeFooter(buffer, footer);
        return buffer.writeDataTo(output);
    }

    @Override
    public int writeRowIndexes(SliceOutput output, List<RowGroupIndex> rowGroupIndexes)
            throws IOException
    {
        buffer.reset();
        metadataWriter.writeRowIndexes(buffer, rowGroupIndexes);
        return buffer.writeDataTo(output);
    }

    @Override
    public MetadataReader getMetadataReader()
    {
        return metadataWriter.getMetadataReader();
    }
}

