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
package com.splicemachine.orc;

import io.airlift.compress.MalformedInputException;
import io.airlift.compress.snappy.SnappyDecompressor;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

class OrcSnappyDecompressor
        implements OrcDecompressor
{
    private final OrcDataSourceId orcDataSourceId;
    private final int maxBufferSize;
    private final SnappyDecompressor decompressor = new SnappyDecompressor();

    public OrcSnappyDecompressor(OrcDataSourceId orcDataSourceId, int maxBufferSize)
    {
        this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSourceId is null");
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public int decompress(byte[] input, int offset, int length, OutputBuffer output)
            throws OrcCorruptionException
    {
        try {
            int uncompressedLength = SnappyDecompressor.getUncompressedLength(input, offset);
            if (uncompressedLength > maxBufferSize) {
                throw new OrcCorruptionException(orcDataSourceId, "Snappy requires buffer (%s) larger than max size (%s)", uncompressedLength, maxBufferSize);
            }

            // Snappy decompressor is more efficient if there's at least a long's worth of extra space
            // in the output buffer
            byte[] buffer = output.initialize(uncompressedLength + SIZE_OF_LONG);
            return decompressor.decompress(input, offset, length, buffer, 0, buffer.length);
        }
        catch (MalformedInputException e) {
            throw new OrcCorruptionException(e, orcDataSourceId, "Invalid compressed stream");
        }
    }

    @Override
    public String toString()
    {
        return "snappy";
    }
}

