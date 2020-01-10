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
package com.splicemachine.orc.stream;

import com.splicemachine.orc.OrcCorruptionException;

import java.io.IOException;
import java.io.InputStream;

final class OrcStreamUtils
{
    public static final int MIN_REPEAT_SIZE = 3;

    private OrcStreamUtils()
    {
    }

    public static void skipFully(InputStream input, long length)
            throws IOException
    {
        while (length > 0) {
            long result = input.skip(length);
            if (result < 0) {
                throw new OrcCorruptionException("Unexpected end of stream");
            }
            length -= result;
        }
    }

    public static void readFully(InputStream input, byte[] buffer, int offset, int length)
            throws IOException
    {
        while (offset < length) {
            int result = input.read(buffer, offset, length - offset);
            if (result < 0) {
                throw new OrcCorruptionException("Unexpected end of stream");
            }
            offset += result;
        }
    }
}
