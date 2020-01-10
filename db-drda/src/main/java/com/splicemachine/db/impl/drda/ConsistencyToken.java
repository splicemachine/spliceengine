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

package com.splicemachine.db.impl.drda;

/**
 * Class which represents an RDB Package Consistency Token.
 */
final class ConsistencyToken {
    /** Byte array representation of the token. */
    private final byte[] bytes;
    /** Cached hash code. */
    private int hash = 0;

    /**
     * Create a new <code>ConsistencyToken</code> instance.
     *
     * @param bytes byte array representing the token
     */
    ConsistencyToken(byte[] bytes) {
        this.bytes = bytes;
    }

    /**
     * Get the byte array representation of the consistency token.
     *
     * @return a <code>byte[]</code> value
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * Check whether this object is equal to another object.
     *
     * @param o another object
     * @return true if the objects are equal
     */
    public boolean equals(Object o) {
        if (!(o instanceof ConsistencyToken)) return false;
        ConsistencyToken ct = (ConsistencyToken) o;
        int len = bytes.length;
        if (len != ct.bytes.length) return false;
        for (int i = 0; i < len; ++i) {
            if (bytes[i] != ct.bytes[i]) return false;
        }
        return true;
    }

    /**
     * Calculate the hash code.
     *
     * @return hash code
     */
    public int hashCode() {
        // ConsistencyToken objects might be kept for a long time and are
        // frequently used as keys in hash tables. Therefore, it is a good idea
        // to cache their hash codes.
        int h = hash;
        if (h == 0) {
            // The hash code has not been calculated yet (or perhaps the hash
            // code actually is 0). Calculate a new one and cache it. No
            // synchronization is needed since reads and writes of 32-bit
            // primitive values are guaranteed to be atomic. See The
            // "Double-Checked Locking is Broken" Declaration for details.
            int len = bytes.length;
            for (int i = 0; i < len; ++i) {
                h ^= bytes[i];
            }
            hash = h;
        }
        return h;
    }

    /**
     * Return a string representation of the consistency token by
     * converting it to a <code>BigInteger</code> value. (For
     * debugging only.)
     *
     * @return a <code>String</code> value
     */
    public String toString() {
        return new java.math.BigInteger(bytes).toString();
    }
}
