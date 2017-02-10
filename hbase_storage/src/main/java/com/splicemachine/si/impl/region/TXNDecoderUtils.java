/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
        		.setTxnId(txnId).setBeginTs(beginTs).setParentTxnid(parentTs);
        if (destTableBuffer !=null)
        	info.setDestinationTables(destTableBuffer);
        if(hasAdditive)
            info = info.setIsAdditive(additive);
        return TxnMessage.Txn.newBuilder().setInfo(info.build())
                .setCommitTs(commitTs).setGlobalCommitTs(globalCommitTs).setState(state.getId())
                .setLastKeepAliveTime(kaTime).addAllRollbackSubIds(rollbackSubIds).build();

    }
}
