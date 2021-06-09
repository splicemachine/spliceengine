/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.storage;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.index.BitIndex;

import java.nio.ByteBuffer;

public class Utils {
    public static void meld(EntryDecoder to, EntryDecoder from, EntryEncoder encoder) {
        BitIndex toBitIndex = to.getCurrentIndex();
        BitIndex fromBitIndex = from.getCurrentIndex();
        MultiFieldDecoder toDecoder = to.getEntryDecoder();
        MultiFieldDecoder fromDecoder = from.getEntryDecoder();
        MultiFieldEncoder multiFieldEncoder = encoder.getEntryEncoder();
        int i = 0;
        int length = Integer.max(toBitIndex.length(), fromBitIndex.length());
        for (i = 0; i < length; ++i) {
            if(fromBitIndex.isSet(i)) {
                ByteBuffer buffer = from.nextAsBuffer(fromDecoder, i).slice();
                multiFieldEncoder.setRawBytes(buffer.array(), buffer.arrayOffset(), buffer.limit());
            } else if(toBitIndex.isSet(i)) {
                ByteBuffer buffer = to.nextAsBuffer(toDecoder, i).slice();
                multiFieldEncoder.setRawBytes(buffer.array(), buffer.arrayOffset(), buffer.limit());
            }
        }
    }
}
