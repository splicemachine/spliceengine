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

package com.splicemachine.hbase;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Scott Fines
 *         Date: 4/15/14
 */
public class SizedInterval implements Comparable<SizedInterval>{
    byte[] startKey;
    byte[] endKey;
    long bytes;

    SizedInterval(byte[] startKey, byte[] endKey, long bytes) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.bytes = bytes;
    }

    @Override
    public int compareTo(SizedInterval o) {
        return Bytes.compareTo(startKey,o.startKey);
    }

    @Override
    public String toString() {
        return "{["+ Bytes.toStringBinary(startKey)+","+Bytes.toStringBinary(endKey)+"):"+bytes+"}";
    }
}
