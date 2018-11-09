/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.si.data.hbase.rollforward;

import com.splicemachine.storage.Partition;
import com.splicemachine.utils.ByteSlice;

public class RFEvent {
    private Partition partition;
    private ByteSlice byteSlice;
    private long txnId;
    private long timestamp;

    public RFEvent(Partition partition, ByteSlice byteSlice, long txnId, long timestamp) {
        this.partition = partition;
        this.byteSlice = byteSlice;
        this.txnId = txnId;
        this.timestamp = timestamp;
    }

    public ByteSlice getByteSlice() {
        return byteSlice;
    }

    public void setByteSlice(ByteSlice byteSlice) {
        this.byteSlice = byteSlice;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public Partition getPartition() {
        return partition;
    }
}
