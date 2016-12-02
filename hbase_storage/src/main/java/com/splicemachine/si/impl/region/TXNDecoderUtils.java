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
 */

package com.splicemachine.si.impl.region;

import com.google.common.primitives.Longs;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.Txn.IsolationLevel;
import com.splicemachine.si.coprocessor.TxnMessage;

import java.util.List;

public class TXNDecoderUtils {

    public static TxnMessage.Txn composeValue(Cell destinationTables, IsolationLevel level, long txnId, long beginTs, long parentTs, boolean hasAdditive,
                                              boolean additive, long commitTs, long globalCommitTs, Txn.State state, long kaTime, List<Long> rollbackSubIds) {
        ByteString destTableBuffer = null;
        if(destinationTables!=null){
            destTableBuffer = ZeroCopyLiteralByteString.wrap(CellUtil.cloneValue(destinationTables));
        }
        if (level == null)
        	level = Txn.IsolationLevel.SNAPSHOT_ISOLATION;
        TxnMessage.TxnInfo.Builder info = TxnMessage.TxnInfo.newBuilder().setIsolationLevel(level.encode())
        		.setTxnId(beginTs).setBeginTs(beginTs).setParentTxnid(parentTs);
        if (destTableBuffer !=null)
        	info.setDestinationTables(destTableBuffer);
        if(hasAdditive)
            info = info.setIsAdditive(additive);
        return TxnMessage.Txn.newBuilder().setInfo(info.build())
                .setCommitTs(commitTs).setGlobalCommitTs(globalCommitTs).setState(state.getId())
                .setLastKeepAliveTime(kaTime).addAllRollbackSubIds(rollbackSubIds).build();

    }
}
