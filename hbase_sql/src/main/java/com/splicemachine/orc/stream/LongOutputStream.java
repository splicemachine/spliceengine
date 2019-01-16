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


import com.splicemachine.orc.checkpoint.LongStreamCheckpoint;
import com.splicemachine.orc.metadata.CompressionKind;
import static com.splicemachine.orc.metadata.Stream.StreamKind.LENGTH;

public interface LongOutputStream
        extends ValueOutputStream<LongStreamCheckpoint>
{
    static LongOutputStream createLengthOutputStream(CompressionKind compression, int bufferSize, boolean isDwrf)
    {
        if (isDwrf) {
            return new LongOutputStreamV1(compression, bufferSize, false, LENGTH);
        }
        else {
            return new LongOutputStreamV2(compression, bufferSize, false, LENGTH);
        }
    }

    void writeLong(long value);
}

