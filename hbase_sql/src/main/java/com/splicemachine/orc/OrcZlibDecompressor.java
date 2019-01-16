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

import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static java.util.Objects.requireNonNull;

class OrcZlibDecompressor
        implements OrcDecompressor
{
    private static final int EXPECTED_COMPRESSION_RATIO = 5;

    private final OrcDataSourceId orcDataSourceId;
    private final int maxBufferSize;

    public OrcZlibDecompressor(OrcDataSourceId orcDataSourceId, int maxBufferSize)
    {
        this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSourceId is null");
        this.maxBufferSize = maxBufferSize;
    }

    public int decompress(byte[] input, int offset, int length, OutputBuffer output)
            throws OrcCorruptionException
    {
        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput(input, offset, length);
            byte[] buffer = output.initialize(Math.min(length * EXPECTED_COMPRESSION_RATIO, maxBufferSize));

            int uncompressedLength = 0;
            while (true) {
                uncompressedLength += inflater.inflate(buffer, uncompressedLength, buffer.length - uncompressedLength);
                if (inflater.finished() || buffer.length >= maxBufferSize) {
                    break;
                }
                int oldBufferSize = buffer.length;
                buffer = output.grow(Math.min(buffer.length * 2, maxBufferSize));
                if (buffer.length <= oldBufferSize) {
                    throw new IllegalStateException(String.format("Buffer failed to grow. Old size %d, current size %d", oldBufferSize, buffer.length));
                }
            }

            if (!inflater.finished()) {
                throw new OrcCorruptionException(orcDataSourceId, "Could not decompress all input (output buffer too small?)");
            }

            return uncompressedLength;
        }
        catch (DataFormatException e) {
            throw new OrcCorruptionException(e, orcDataSourceId, "Invalid compressed stream");
        }
        finally {
            inflater.end();
        }
    }

    @Override
    public String toString()
    {
        return "zlib";
    }
}

