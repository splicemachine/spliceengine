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

import io.airlift.compress.Compressor;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

import static java.util.zip.Deflater.FULL_FLUSH;

public class DeflateCompressor
        implements Compressor
{
    private static final int EXTRA_COMPRESSION_SPACE = 16;
    private static final int COMPRESSION_LEVEL = 4;

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        // From Mark Adler's post http://stackoverflow.com/questions/1207877/java-size-of-compression-output-bytearray
        return uncompressedSize + ((uncompressedSize + 7) >> 3) + ((uncompressedSize + 63) >> 6) + 5 + EXTRA_COMPRESSION_SPACE;
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        int maxCompressedLength = maxCompressedLength(inputLength);
        if (maxOutputLength < maxCompressedLength) {
            throw new IllegalArgumentException("Output buffer must be at least " + maxCompressedLength + " bytes");
        }

        Deflater deflater = new Deflater(COMPRESSION_LEVEL, true);

        deflater.setInput(input, inputOffset, inputLength);
        deflater.finish();

        int compressedDataLength = deflater.deflate(output, outputOffset, maxOutputLength, FULL_FLUSH);
        if (!deflater.finished()) {
            throw new IllegalStateException("maxCompressedLength formula is incorrect, because deflate produced more data");
        }
        deflater.end();
        return compressedDataLength;
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        throw new UnsupportedOperationException("Compression of byte buffer not supported for deflate");
    }
}

