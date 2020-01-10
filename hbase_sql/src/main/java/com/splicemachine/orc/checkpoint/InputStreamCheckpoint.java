/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
package com.splicemachine.orc.checkpoint;

import com.splicemachine.orc.checkpoint.Checkpoints.ColumnPositionsList;
import com.splicemachine.orc.metadata.CompressionKind;

import static com.splicemachine.orc.metadata.CompressionKind.UNCOMPRESSED;
import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * InputStreamCheckpoint is represented as a packed long to avoid object creation in inner loops.
 */
public final class InputStreamCheckpoint
{
    private InputStreamCheckpoint()
    {
    }

    public static long createInputStreamCheckpoint(CompressionKind compressionKind, ColumnPositionsList positionsList)
    {
        if (compressionKind == UNCOMPRESSED) {
            return createInputStreamCheckpoint(0, positionsList.nextPosition());
        }
        else {
            return createInputStreamCheckpoint(positionsList.nextPosition(), positionsList.nextPosition());
        }
    }

    public static long createInputStreamCheckpoint(int compressedBlockOffset, int decompressedOffset)
    {
        return (((long) compressedBlockOffset) << 32) | decompressedOffset;
    }

    public static int decodeCompressedBlockOffset(long inputStreamCheckpoint)
    {
        return ((int) (inputStreamCheckpoint >> 32));
    }

    public static int decodeDecompressedOffset(long inputStreamCheckpoint)
    {
        // low order bits contain the decompressed offset, so a simple cast here will suffice
        return (int) inputStreamCheckpoint;
    }

    public static String inputStreamCheckpointToString(long inputStreamCheckpoint)
    {
        return toStringHelper(InputStreamCheckpoint.class)
                .add("decompressedOffset", decodeDecompressedOffset(inputStreamCheckpoint))
                .add("compressedBlockOffset", decodeCompressedBlockOffset(inputStreamCheckpoint))
                .toString();
    }
}
