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

package com.splicemachine.si.impl.region;

import com.google.protobuf.ByteString;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.TxnUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Decoder which decodes Transactions stored in the TxnStore.
 *
 * @author Scott Fines
 * Date: 8/18/14
 */
@SuppressFBWarnings(value = "MS_PKGPROTECT", justification = "intentional, static fields are read by other components.")
public class V2TxnDecoder implements TxnDecoder{

    public static final V2TxnDecoder INSTANCE=new V2TxnDecoder();

    public static final byte[] FAMILY=SIConstants.DEFAULT_FAMILY_BYTES;
    static final byte[] DATA_QUALIFIER_BYTES=Bytes.toBytes("d");
    static final byte[] KEEP_ALIVE_QUALIFIER_BYTES=Bytes.toBytes("k");
    public static final byte[] COMMIT_QUALIFIER_BYTES=Bytes.toBytes("t");
    public static final byte[] GLOBAL_COMMIT_QUALIFIER_BYTES=Bytes.toBytes("g");
    static final byte[] STATE_QUALIFIER_BYTES=Bytes.toBytes("s");
    static final byte[] DESTINATION_TABLE_QUALIFIER_BYTES=Bytes.toBytes("e"); //had to pick a letter that was unique
    static final byte[] ROLLBACK_SUBTRANSACTIONS_QUALIFIER_BYTES=Bytes.toBytes("r"); //had to pick a letter that was unique
    static final byte[] TASK_QUALIFIER_BYTES=Bytes.toBytes("v"); //had to pick a letter that was unique
    static final byte[] CONFLICTING_TXN_IDS_BYTES =Bytes.toBytes("w"); //had to pick a letter that was unique

    private V2TxnDecoder(){ } //singleton instance

    @Override
    public TxnMessage.Txn decode(RegionTxnStore txnStore, List<Cell> keyValues) {

        if (keyValues.size() <= 0) {
            return null;
        }

        Cell dataKv = null;
        Cell keepAliveKv = null;
        Cell commitKv = null;
        Cell globalCommitKv = null;
        Cell stateKv = null;
        Cell destinationTables = null;
        Cell rollbackIds = null;
        Cell taskId = null;
        Cell conflictingTxnIds = null;

        for (Cell kv : keyValues) {
            if (CellUtils.singleMatchingColumn(kv, FAMILY, DATA_QUALIFIER_BYTES))
                dataKv = kv;
            else if (CellUtils.singleMatchingColumn(kv, FAMILY, KEEP_ALIVE_QUALIFIER_BYTES))
                keepAliveKv = kv;
            else if (CellUtils.singleMatchingColumn(kv, FAMILY, GLOBAL_COMMIT_QUALIFIER_BYTES))
                globalCommitKv = kv;
            else if (CellUtils.singleMatchingColumn(kv, FAMILY, COMMIT_QUALIFIER_BYTES))
                commitKv = kv;
            else if (CellUtils.singleMatchingColumn(kv, FAMILY, STATE_QUALIFIER_BYTES))
                stateKv = kv;
            else if (CellUtils.singleMatchingColumn(kv, FAMILY, DESTINATION_TABLE_QUALIFIER_BYTES))
                destinationTables = kv;
            else if (CellUtils.singleMatchingColumn(kv, FAMILY, ROLLBACK_SUBTRANSACTIONS_QUALIFIER_BYTES))
                rollbackIds = kv;
            else if (CellUtils.singleMatchingColumn(kv, FAMILY, TASK_QUALIFIER_BYTES))
                taskId = kv;
            else if (CellUtils.singleMatchingColumn(kv, FAMILY, CONFLICTING_TXN_IDS_BYTES))
                conflictingTxnIds = kv;
        }

        if (dataKv == null) {
            return null;
        }

        long txnId = TxnUtils.txnIdFromRowKey(dataKv.getRowArray(), dataKv.getRowOffset(), dataKv.getRowLength());

        return decodeInternal(txnStore, dataKv, keepAliveKv, commitKv, globalCommitKv, stateKv, destinationTables, txnId, rollbackIds, taskId, conflictingTxnIds);
    }

    @Override
    public TxnMessage.TaskId decodeTaskId(RegionTxnStore txnStore, long txnId, Result result) {
        Cell taskKv=result.getColumnLatestCell(FAMILY,TASK_QUALIFIER_BYTES);
        return decodeTaskId(taskKv);
    }

    @Override
    public TxnMessage.ConflictingTxnIdsResponse decodeConflictingTxnIds(long txnId, Result result) {
        Cell conflictingTxnIds = result.getColumnLatestCell(FAMILY, CONFLICTING_TXN_IDS_BYTES);
        return TxnMessage.ConflictingTxnIdsResponse.newBuilder().addAllConflictingTxnIds(decodeLongArray(conflictingTxnIds)).build();
    }

    @Override
    public TxnMessage.Txn decode(RegionTxnStore txnStore, long txnId, Result result) {

        Cell dataKv = result.getColumnLatestCell(FAMILY, DATA_QUALIFIER_BYTES);
        if (dataKv == null) {
            return null;
        }

        Cell commitTsVal = result.getColumnLatestCell(FAMILY, COMMIT_QUALIFIER_BYTES);
        Cell globalTsVal = result.getColumnLatestCell(FAMILY, GLOBAL_COMMIT_QUALIFIER_BYTES);
        Cell stateKv = result.getColumnLatestCell(FAMILY, STATE_QUALIFIER_BYTES);
        Cell destinationTables = result.getColumnLatestCell(FAMILY, DESTINATION_TABLE_QUALIFIER_BYTES);
        Cell kaTime = result.getColumnLatestCell(FAMILY, KEEP_ALIVE_QUALIFIER_BYTES);
        Cell rollbackIds = result.getColumnLatestCell(FAMILY, ROLLBACK_SUBTRANSACTIONS_QUALIFIER_BYTES);
        Cell taskKv = result.getColumnLatestCell(FAMILY, TASK_QUALIFIER_BYTES);
        Cell conflictingTxnIds = result.getColumnLatestCell(FAMILY, CONFLICTING_TXN_IDS_BYTES);

        return decodeInternal(txnStore, dataKv, kaTime, commitTsVal, globalTsVal, stateKv, destinationTables, txnId, rollbackIds, taskKv, conflictingTxnIds);
    }

    @Override
    public TxnMessage.Txn decodeV1(RegionTxnStore txnStore, long txnId,Result result) {
        Cell dataKv=result.getColumnLatestCell(FAMILY,DATA_QUALIFIER_BYTES);
        Cell commitTsVal=result.getColumnLatestCell(FAMILY,COMMIT_QUALIFIER_BYTES);
        Cell globalTsVal=result.getColumnLatestCell(FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES);
        Cell stateKv=result.getColumnLatestCell(FAMILY,STATE_QUALIFIER_BYTES);
        Cell destinationTables=result.getColumnLatestCell(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES);
        Cell kaTime=result.getColumnLatestCell(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES);
        if(dataKv==null) return null;
        return decodeInternalV1(txnStore,dataKv,kaTime,commitTsVal,globalTsVal,stateKv,destinationTables,txnId);
    }

    /**
     * Encodes transaction objects using the new, packed Encoding format
     *
     * The new way is a (more) compact representation which uses the Values CF (V) and compact qualifiers (using
     * the Encoding.encodeX() methods) as follows:
     *
     * "d" -- packed tuple of (beginTimestamp,parentTxnId,isDependent,additive,isolationLevel) // Youssef: looks like isDependent is not encoded?
     * "c" -- counter (using a packed integer representation)
     * "k" -- keepAlive timestamp
     * "t" -- commit timestamp
     * "g" -- globalCommitTimestamp
     * "s" -- state
     * "v" -- taskId
     * "w" -- conflictingTxnIds
     *
     * The additional columns are kept separate so that they may be updated(and read) independently without
     * reading and decoding the entire transaction.
     *
     * In the new format, if a transaction has been written to the table, then it automatically allows writes
     *
     * order: c,d,e,g,k,s,t,v
     * order: counter,data,destinationTable,globalCommitTimestamp,keepAlive,state,commitTimestamp,taskId
     */
    @Override
    public org.apache.hadoop.hbase.client.Put encodeForPut(TxnMessage.TxnInfo txnInfo, byte[] rowKey, Clock clock) throws IOException {

        MultiFieldEncoder encoder = MultiFieldEncoder.create(5);

        // 1. encode begin timestamp
        encoder.encodeNext(txnInfo.getBeginTs())
        // 2. encode parent transaction id
               .encodeNext(txnInfo.getParentTxnid());
        // 3. encode additivity
        if (txnInfo.hasIsAdditive()) {
            encoder.encodeNext(txnInfo.getIsAdditive());
        } else {
            encoder.encodeEmpty();
        }
        // 4. encode isolation level
        Txn.IsolationLevel level = Txn.IsolationLevel.fromInt(txnInfo.getIsolationLevel());
        if (level != null) {
            encoder.encodeNext(level.encode());
        } else {
            encoder.encodeEmpty();
        }

        org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(rowKey);

        // 1. column DATA_QUALIFIER_BYTES
        put.addColumn(FAMILY, DATA_QUALIFIER_BYTES, encoder.build());

        // 2. column KEEP_ALIVE_QUALIFIER_BYTES
        put.addColumn(FAMILY, KEEP_ALIVE_QUALIFIER_BYTES, Encoding.encode(clock.currentTimeMillis()));

        // 3. column STATE_QUALIFIER_BYTES
        put.addColumn(FAMILY, STATE_QUALIFIER_BYTES, Txn.State.ACTIVE.encode());

        // 4. column DESTINATION_TABLE_QUALIFIER_BYTES
        ByteString destTableBuffer = txnInfo.getDestinationTables();
        if (destTableBuffer != null && !destTableBuffer.isEmpty()) {
            put.addColumn(FAMILY, DESTINATION_TABLE_QUALIFIER_BYTES, destTableBuffer.toByteArray());
        }

        // 5. column TASK_QUALIFIER_BYTES
        if (txnInfo.hasTaskId()) {
            TxnMessage.TaskId taskId = txnInfo.getTaskId();
            MultiFieldEncoder taskIdEncoder = MultiFieldEncoder.create(5);
            taskIdEncoder.encodeNext("")
                         .encodeNext(0)
                         .encodeNext(taskId.getStageId())
                         .encodeNext(taskId.getPartitionId())
                         .encodeNext(taskId.getTaskAttemptNumber());
            put.addColumn(FAMILY, TASK_QUALIFIER_BYTES, taskIdEncoder.build());
        }

        // 6. column CONFLICTING_TXN_IDS
        List<Long> conflictingTxnIdsBuffer = txnInfo.getConflictingTxnIdsList();
        if (!conflictingTxnIdsBuffer.isEmpty()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            boolean first = true;
            for (long id : txnInfo.getConflictingTxnIdsList()) {
                if (!first) {
                    baos.write(0);
                }
                baos.write(Encoding.encode(id));
                first = false;
            }
            put.addColumn(FAMILY, DESTINATION_TABLE_QUALIFIER_BYTES, baos.toByteArray());
        }

        return put;
    }

    /**
     * @param txnStore used to adjust state of a transaction changing it from ACTIVE to ROLLBACK if keepalive period is exceeded.
     *                 if null, adjustment will not take place.
     */
    protected TxnMessage.Txn decodeInternal(RegionTxnStore txnStore,
                                            Cell dataKv, Cell keepAliveKv, Cell commitKv, Cell globalCommitKv,
                                            Cell stateKv, Cell destinationTables, long txnId, Cell rollbackIds,
                                            Cell taskKv, Cell conflictingTxnIds) {

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(dataKv.getValueArray(), dataKv.getValueOffset(), dataKv.getValueLength());
        long beginTs = decoder.decodeNextLong();
        long parentTxnId = -1L;
        if (!decoder.nextIsNull()) parentTxnId = decoder.decodeNextLong();
        else decoder.skip();

        boolean isAdditive = false;
        boolean hasAdditive = true;
        if (!decoder.nextIsNull()) {
            isAdditive = decoder.decodeNextBoolean();
        } else {
            hasAdditive = false;
            decoder.skip();
        }

        Txn.IsolationLevel level = null;
        if (!decoder.nextIsNull()) {
            level = Txn.IsolationLevel.fromByte(decoder.decodeNextByte());
        } else {
            decoder.skip();
        }

        long commitTs = -1L;
        if (commitKv != null) {
            commitTs = toLong(commitKv);
        }

        long globalTs = -1L;
        if (globalCommitKv != null) {
            globalTs = toLong(globalCommitKv);
        }

        Txn.State state = Txn.State.decode(stateKv.getValueArray(), stateKv.getValueOffset(), stateKv.getValueLength());

        // adjust state for committed timestamp
        if (commitTs > 0 || globalTs > 0) {
            //we have a commit timestamp, our state MUST be committed
            state = Txn.State.COMMITTED;
        }

        // check if the transaction timed out
        boolean timedOut = false;
        if (state == Txn.State.ACTIVE && txnStore != null) {
            /*
             * We need to check that the transaction hasn't been timed out (and therefore rolled back). This
             * happens if the keepAliveTime is older than the configured transaction timeout. Of course,
             * there is some network latency which could cause small keep alives to be problematic. To help out,
             * we allow a little fudge factor in the timeout
             */
            state = txnStore.adjustStateForTimeout(state, keepAliveKv);
            timedOut = state != Txn.State.ACTIVE;
        }

        long keepAliveTime = decodeKeepAlive(keepAliveKv, false);

        List<Long> rollbackSubIds = decodeLongArray(rollbackIds);
        long subId = txnId & SIConstants.SUBTRANSANCTION_ID_MASK;
        if (subId != 0 && rollbackSubIds.contains(subId)) {
            state = Txn.State.ROLLEDBACK;
        }

        TxnMessage.TaskId taskId = null;
        if (taskKv != null) {
            taskId = decodeTaskId(taskKv);
        }

        List<Long> conflictingTxnIdsList = decodeLongArray(conflictingTxnIds);

        TxnMessage.Txn decodedTxn = composeValue(destinationTables, conflictingTxnIdsList, level, txnId, beginTs, parentTxnId, hasAdditive,
                                                 isAdditive, commitTs, globalTs, state, keepAliveTime, rollbackSubIds,
                                                 taskId);

        boolean committedWithoutGlobalTS = state == Txn.State.COMMITTED && parentTxnId > 0 && globalTs < 0;
        if ((timedOut || committedWithoutGlobalTS) && txnStore != null) {
            txnStore.resolveTxn(decodedTxn);
        }

        return decodedTxn;
    }

    private static TxnMessage.Txn composeValue(Cell destinationTables, List<Long> conflictingTxnIds, Txn.IsolationLevel level,
                                          long txnId, long beginTs, long parentTs, boolean hasAdditive, boolean additive,
                                          long commitTs, long globalCommitTs, Txn.State state, long kaTime,
                                          List<Long> rollbackSubIds, TxnMessage.TaskId taskId) {
        return TXNDecoderUtils.composeValue(destinationTables, conflictingTxnIds, level, txnId, beginTs, parentTs, hasAdditive, additive,
                                            commitTs, globalCommitTs, state, kaTime, rollbackSubIds, taskId);
    }

    private static TxnMessage.Txn decodeInternalV1(RegionTxnStore txnStore, Cell dataKv, Cell keepAliveKv, Cell commitKv,
                                              Cell globalCommitKv, Cell stateKv, Cell destinationTables, long txnId) {

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(dataKv.getValueArray(), dataKv.getValueOffset(), dataKv.getValueLength());
        long beginTs = decoder.decodeNextLong();
        long parentTxnId = -1L;
        if (!decoder.nextIsNull()) {
            parentTxnId = decoder.decodeNextLong();
        } else {
            decoder.skip();
        }

        boolean isAdditive = false;
        boolean hasAdditive = true;
        if (!decoder.nextIsNull()) {
            isAdditive = decoder.decodeNextBoolean();
        } else {
            hasAdditive = false;
            decoder.skip();
        }

        Txn.IsolationLevel level = null;
        if (!decoder.nextIsNull()) {
            level = Txn.IsolationLevel.fromByte(decoder.decodeNextByte());
        } else {
            decoder.skip();
        }

        long commitTs = -1L;
        if (commitKv != null) {
            commitTs = toLong(commitKv);
        }

        long globalTs = -1L;
        if (globalCommitKv != null) {
            globalTs = toLong(globalCommitKv);
        }

        Txn.State state = Txn.State.decode(stateKv.getValueArray(), stateKv.getValueOffset(), stateKv.getValueLength());
        // adjust for committed timestamp
        if (commitTs > 0 || globalTs > 0) {
            //we have a commit timestamp, our state MUST be committed
            state = Txn.State.COMMITTED;
        }
        if (state == Txn.State.ACTIVE) {
            /*
             * We need to check that the transaction hasn't been timed out (and therefore rolled back). This
             * happens if the keepAliveTime is older than the configured transaction timeout. Of course,
             * there is some network latency which could cause small keep alives to be problematic. To help out,
             * we allow a little fudge factor in the timeout
             */
            state = txnStore.adjustStateForTimeout(state, keepAliveKv);
        }

        long kaTime = decodeKeepAlive(keepAliveKv, false);

        return composeValue(destinationTables, null, level, txnId, beginTs, parentTxnId, hasAdditive,
                            isAdditive, commitTs, globalTs, state, kaTime, Collections.emptyList(), null);
    }

    private static List<Long> decodeLongArray(Cell input) {
        if (input == null)
            return Collections.emptyList();
        List<Long> ids = new ArrayList<>();
        MultiFieldDecoder decoder=MultiFieldDecoder.wrap(input.getValueArray(), input.getValueOffset(), input.getValueLength());
        while (decoder.available()) {
            ids.add(decoder.decodeNextLong());
        }
        return ids;
    }

    public static long decodeKeepAlive(Cell columnLatest,boolean oldForm){
        long lastKATime;
        if(oldForm){
            /*
             * The old version would put an empty value into the Keep Alive column. If the transaction
             * committed before the keep alive was initiated, then the field will still be null.
             *
             * Since we only read transactions in the old form, and don't create new ones, we just have to decide
             * what to do with these situations. They can only arise if the transaction either
             *
             * A) commits/rolls back before the keep alive can be initiated
             * B) fails before the first keep alive.
             *
             * In the case of a commit/roll back, the value of the keep alive doesn't matter, and in the case
             * of B), we want to fail it. The easiest way to deal with this is just to return 0l.
             */
            int length=columnLatest.getValueLength();
            if(length==0) return 0L;
            else
                lastKATime=Bytes.toLong(columnLatest.getValueArray(),columnLatest.getValueOffset(),length);
        }else
            lastKATime=Encoding.decodeLong(columnLatest.getValueArray(),columnLatest.getValueOffset(),false);
        return lastKATime;
    }

    private static TxnMessage.TaskId decodeTaskId(Cell taskKv) {

        if (taskKv == null) {
            return null;
        }

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(taskKv.getValueArray(), taskKv.getValueOffset(), taskKv.getValueLength());
        decoder.decodeNextString(); // ignored
        decoder.decodeNextInt(); // ignored
        int stageId = decoder.decodeNextInt();
        int partitionId = decoder.decodeNextInt();
        int attemptNumber = decoder.decodeNextInt();

        return TxnMessage.TaskId
                .newBuilder()
                .setStageId(stageId)
                .setPartitionId(partitionId)
                .setTaskAttemptNumber(attemptNumber)
                .build();
    }

    private static long toLong(Cell data){
        return Encoding.decodeLong(data.getValueArray(),data.getValueOffset(),false);
    }
}
