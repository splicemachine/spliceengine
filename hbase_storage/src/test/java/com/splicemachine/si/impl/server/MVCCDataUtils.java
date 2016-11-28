/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.si.impl.server;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Scott Fines
 *         Date: 11/28/16
 */
class MVCCDataUtils{

    public static Cell insert(Txn insertTxn){
        return insert(insertTxn,Bytes.toBytes("hello"));
    }

    public static Cell insert(Txn insertTxn,byte[] value){
        return new KeyValue(Bytes.toBytes("row1"),
                SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.PACKED_COLUMN_BYTES,
                insertTxn.getBeginTimestamp(),
                value);
    }

    public static  Cell tombstone(Txn deleteTxn){
        return new KeyValue(Bytes.toBytes("row1"),
                SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                deleteTxn.getBeginTimestamp(),
                Bytes.toBytes(""));
    }

    public static Cell antiTombstone(Txn txn){
        return new KeyValue(Bytes.toBytes("row1"),
                SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                txn.getBeginTimestamp(),
                SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES);
    }

    public static Cell commitTimestamp(Txn txn){
        return new KeyValue(Bytes.toBytes("row1"),
                SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
                txn.getBeginTimestamp(),
                Bytes.toBytes(txn.getEffectiveCommitTimestamp()));
    }
}
