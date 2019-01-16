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

import com.splicemachine.orc.metadata.CompressionKind;

import java.util.Optional;

public interface OrcDecompressor
{
    static Optional<OrcDecompressor> createOrcDecompressor(OrcDataSourceId orcDataSourceId, CompressionKind compression, int bufferSize)
            throws OrcCorruptionException
    {
        switch (compression) {
            case NONE:
                return Optional.empty();
            case ZLIB:
                return Optional.of(new OrcZlibDecompressor(orcDataSourceId, bufferSize));
            case SNAPPY:
                return Optional.of(new OrcSnappyDecompressor(orcDataSourceId, bufferSize));
            case ZSTD:
                return Optional.of(new OrcZstdDecompressor(orcDataSourceId, bufferSize));
            default:
                throw new OrcCorruptionException(orcDataSourceId, "Unknown compression type: " + compression);
        }
    }

    int decompress(byte[] input, int offset, int length, OutputBuffer output)
            throws OrcCorruptionException;

    interface OutputBuffer
    {
        byte[] initialize(int size);

        byte[] grow(int size);
    }
}

