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

import com.splicemachine.db.shared.common.sanity.SanityManager;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.index.BitIndex;

import java.nio.ByteBuffer;

public class Utils {

    /**
     * Melds, or merges, entries of an {@link EntryDecoder} into another {@link EntryDecoder}
     * writing the result into an {@link EntryEncoder}.
     *
     * @param to the target decoder that we want to merge the columns into.
     * @param from the decoder containing columns we want to meld.
     * @param encoder an inout parameter that holds the results of melding.
     *
     * Example:
     *    to = { {INTEGER, 42}, {STRING, "foo"}, {Byte, 0x14}, {FLOAT, 3.14} }
     *    from = < {{STRING, "bar"}, {FLOAT, 6.28}}, set columns are {1,3}  >
     *    encoder = { {INTEGER, 42}, {STRING, "bar"}, {Byte, 0x14}, {FLOAT, 6.28} }
     */
    public static void meld(EntryDecoder to, EntryDecoder from, EntryEncoder encoder) {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(to != null);
            SanityManager.ASSERT(from != null);
            SanityManager.ASSERT(encoder != null);
        }

        BitIndex toBitIndex = to.getCurrentIndex();
        BitIndex fromBitIndex = from.getCurrentIndex();
        MultiFieldDecoder toDecoder = to.getEntryDecoder();
        MultiFieldDecoder fromDecoder = from.getEntryDecoder();
        MultiFieldEncoder multiFieldEncoder = encoder.getEntryEncoder();
        int length = Integer.max(toBitIndex.length(), fromBitIndex.length());
        for (int i = 0; i < length; ++i) {
            ByteBuffer buffer = null;
            if(fromBitIndex.isSet(i)) {
                buffer = from.nextAsBuffer(fromDecoder, i);
                if(toBitIndex.isSet(i)) {
                    to.seekForward(toDecoder,i);
                }
                if(buffer == null) {
                    multiFieldEncoder.encodeEmpty();
                } else {
                    buffer = buffer.slice();
                    multiFieldEncoder.setRawBytes(buffer.array(), buffer.arrayOffset(), buffer.limit());
                }
            } else if(toBitIndex.isSet(i)) {
                buffer = to.nextAsBuffer(toDecoder, i);
                if(buffer == null) {
                    multiFieldEncoder.encodeEmpty();
                } else {
                    buffer = buffer.slice();
                    multiFieldEncoder.setRawBytes(buffer.array(), buffer.arrayOffset(), buffer.limit());
                }
            }
        }
    }
}
