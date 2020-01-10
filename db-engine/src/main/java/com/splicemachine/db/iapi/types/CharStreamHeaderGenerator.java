/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.types;

import java.io.IOException;
import java.io.ObjectOutput;

/**
 * Generates stream headers for non-Clob string data types.
 * <p>
 * The stream header encodes the byte length of the stream. Since two bytes
 * are used for the header, the maximum encodable length is 65535 bytes. There
 * are three special cases, all handled by encoding zero into the header and
 * possibly appending an EOF-marker to the stream:
 * <ul> <li>Unknown length - with EOF marker</li>
 *      <li>Length longer than maximum encodable length - with EOF marker</li>
 *      <li>Length of zero - no EOF marker</li>
 * </ul>
 * The length is encoded like this:
 * <pre>
            out.writeByte((byte)(byteLength >>> 8));
            out.writeByte((byte)(byteLength >>> 0));
 * </pre>
 */
//@Immutable
public final class CharStreamHeaderGenerator
    implements StreamHeaderGenerator {

    /** The maximum length that can be encoded by the header. */
    private static final int MAX_ENCODABLE_LENGTH = 65535;

    /**
     * A byte count is expected.
     *
     * @return {@code false}.
     */
    public boolean expectsCharCount() {
        return false;
    }

    /**
     * Generates the header for the specified length and writes it into the
     * provided buffer, starting at the specified offset.
     *
     * @param buffer the buffer to write into
     * @param offset starting offset in the buffer
     * @param byteLength the length to encode in the header
     * @return The number of bytes written into the buffer.
     */
    public int generateInto(byte[] buffer, int offset, long byteLength) {
        if (byteLength > 0 && byteLength <= MAX_ENCODABLE_LENGTH) {
            buffer[offset] = (byte)(byteLength >>> 8);
            buffer[offset +1] = (byte)byteLength;
        } else {
            // Byte length is zero, unknown or too large to encode.
            buffer[offset] = 0x00;
            buffer[offset +1] = 0x00;
        }
        return 2;
    }

    /**
     * Generates the header for the specified length.
     *
     * @param out the destination stream
     * @param byteLength the byte length to encode in the header
     * @return The number of bytes written to the destination stream.
     * @throws IOException if writing to the destination stream fails
     */
    public int generateInto(ObjectOutput out, long byteLength)
            throws IOException {
        if (byteLength > 0 && byteLength <= MAX_ENCODABLE_LENGTH) {
            out.writeByte((byte)(byteLength >>> 8));
            out.writeByte((byte)(byteLength >>> 0));
        } else {
            // Byte length is zero, unknown or too large to encode.
            out.writeByte(0x00);
            out.writeByte(0x00);
        }
        return 2;
    }

    /**
     * Writes a Derby-specific end-of-stream marker to the buffer for a stream
     * of the specified byte length, if required.
     *
     * @param buffer the buffer to write into
     * @param offset starting offset in the buffer
     * @param byteLength the byte length of the stream
     * @return Number of bytes written (zero or more).
     */
    public int writeEOF(byte[] buffer, int offset, long byteLength) {
        if (byteLength < 0 || byteLength > MAX_ENCODABLE_LENGTH) {
            System.arraycopy(DERBY_EOF_MARKER, 0,
                             buffer, offset, DERBY_EOF_MARKER.length);
            return DERBY_EOF_MARKER.length;
        } else {
            return 0;
        }
    }

    /**
     * Writes a Derby-specific end-of-stream marker to the destination stream
     * for the specified byte length, if required.
     *
     * @param out the destination stream
     * @param byteLength the length of the stream
     * @return Number of bytes written (zero or more).
     */
    public int writeEOF(ObjectOutput out, long byteLength)
            throws IOException {
        if (byteLength < 0 || byteLength > MAX_ENCODABLE_LENGTH) {
            out.write(DERBY_EOF_MARKER);
            return DERBY_EOF_MARKER.length;
        } else {
            return 0;
        }
    }

    /**
     * Returns the maximum header length.
     *
     * @return Maximum header length in bytes.
     */
    public int getMaxHeaderLength() {
        return 2;
    }
}
