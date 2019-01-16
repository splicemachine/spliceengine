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
import io.airlift.compress.zstd.ZstdDecompressor;

import static java.lang.StrictMath.toIntExact;
import static java.util.Objects.requireNonNull;

class OrcZstdDecompressor
        implements OrcDecompressor
{
    private final OrcDataSourceId orcDataSourceId;
    private final int maxBufferSize;
    private final ZstdDecompressor decompressor = new ZstdDecompressor();

    public OrcZstdDecompressor(OrcDataSourceId orcDataSourceId, int maxBufferSize)
    {
        this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSourceId is null");
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public int decompress(byte[] input, int offset, int length, OutputBuffer output)
            throws OrcCorruptionException
    {
        try {
            long uncompressedLength = ZstdDecompressor.getDecompressedSize(input, offset, length);
            if (uncompressedLength > maxBufferSize) {
                throw new OrcCorruptionException(orcDataSourceId, "Zstd requires buffer (%s) larger than max size (%s)", uncompressedLength, maxBufferSize);
            }

            byte[] buffer = output.initialize(toIntExact(uncompressedLength));
            return decompressor.decompress(input, offset, length, buffer, 0, buffer.length);
        }
        catch (MalformedInputException e) {
            throw new OrcCorruptionException(e, orcDataSourceId, "Invalid compressed stream");
        }
    }

    @Override
    public String toString()
    {
        return "zstd";
    }
}

