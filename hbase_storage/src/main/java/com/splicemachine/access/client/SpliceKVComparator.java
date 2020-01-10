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

package com.splicemachine.access.client;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.RawComparator;

/**
 *
 * KeyValueComparator used by the Client Side Merge of Store File Scanners and MemstoreKeyValueScanner.  This
 * class handles the special key values returned from the MemstoreFlushAwareScanner.
 *
 * Created by jleach on 4/12/16.
 */
public class SpliceKVComparator extends KeyValue.KVComparator implements RawComparator<Cell>, KeyValue.SamePrefixComparator<byte[]> {
    public static final SpliceKVComparator INSTANCE = new SpliceKVComparator(KeyValue.COMPARATOR);
    protected KeyValue.KVComparator kvComparator;

    private SpliceKVComparator(KeyValue.KVComparator kvComparator) {
        this.kvComparator = kvComparator;
    }

    @Override
    public int compare(byte[] bytes, int i, int i2, byte[] bytes2, int i3, int i4) {
        return kvComparator.compare(bytes,i,i2,bytes2,i3,i4);
    }

    @Override
    public int compare(Cell o1, Cell o2) {
        // Generated Timestamp Check
        if (o1.getTimestamp() == 0l)
            return -1;
        else if (o2.getTimestamp() == 0l)
            return 1;
        else if (o1.getTimestamp() == HConstants.LATEST_TIMESTAMP)
            return 1;
        else if (o2.getTimestamp() == HConstants.LATEST_TIMESTAMP)
            return -1;
        return kvComparator.compare(o1,o2);
    }

    @Override
    public int compareIgnoringPrefix(int i, byte[] bytes, int i2, int i3, byte[] bytes2, int i4, int i5) {
        return kvComparator.compareIgnoringPrefix(i,bytes,i2,i3,bytes2,i4,i5);
    }
}

