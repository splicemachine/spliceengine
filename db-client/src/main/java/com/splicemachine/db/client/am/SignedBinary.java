/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */
package com.splicemachine.db.client.am;

public class SignedBinary {
    // Hide the default constructor, this is a static class.
    private SignedBinary() {
    }

    /**
     * Unix byte-order for signed binary representations.
     */
    public final static int BIG_ENDIAN = 1;

    /**
     * Intel 80/86 reversed byte-order for signed binary representations.
     */
    public final static int LITTLE_ENDIAN = 2;

    /**
     * Get a byte from the buffer.
     */
    public static final byte getByte(byte[] buffer, int offset) {
        return buffer[ offset ];
    }

    /**
     * Build a Java short from a 2-byte signed binary representation.
     *
     * @throws IllegalArgumentException if the specified byte order is not recognized.
     */
    public static final short getShort(byte[] buffer, int offset) {
        return (short) (((buffer[offset + 0] & 0xff) << 8) +
                ((buffer[offset + 1] & 0xff) << 0));
    }

    /**
     * Build a Java int from a 4-byte signed binary representation.
     *
     * @throws IllegalArgumentException if the specified byte order is not recognized.
     */
    public static final int getInt(byte[] buffer, int offset) {
        return (int) (((buffer[offset + 0] & 0xff) << 24) +
                ((buffer[offset + 1] & 0xff) << 16) +
                ((buffer[offset + 2] & 0xff) << 8) +
                ((buffer[offset + 3] & 0xff) << 0));
    }

    /**
     * Build a Java long from an 8-byte signed binary representation.
     *
     * @throws IllegalArgumentException if the specified byte order is not recognized.
     */
    public static final long getLong(byte[] buffer, int offset) {
        return (long) (((buffer[offset + 0] & 0xffL) << 56) +
                ((buffer[offset + 1] & 0xffL) << 48) +
                ((buffer[offset + 2] & 0xffL) << 40) +
                ((buffer[offset + 3] & 0xffL) << 32) +
                ((buffer[offset + 4] & 0xffL) << 24) +
                ((buffer[offset + 5] & 0xffL) << 16) +
                ((buffer[offset + 6] & 0xffL) << 8) +
                ((buffer[offset + 7] & 0xffL) << 0));
    }

}
