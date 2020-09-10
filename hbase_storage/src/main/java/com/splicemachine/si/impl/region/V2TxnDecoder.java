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

/**
 * Decoder which decodes Transactions stored in the
 *
 * @author Scott Fines
 * Date: 8/18/14
 */

import com.google.protobuf.ByteString;
import com.splicemachine.encoding.DecodingIterator;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.ByteSlice;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import splice.com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

    private V2TxnDecoder(){ } //singleton instance

    @Override
    public TxnMessage.Txn decode(RegionTxnStore txnStore,
                                 List<Cell> keyValues) throws IOException{
        if(keyValues.size()<=0) return null;
        Cell dataKv=null;
        Cell keepAliveKv=null;
        Cell commitKv=null;
        Cell globalCommitKv=null;
        Cell stateKv=null;
        Cell destinationTables=null;
        Cell rollbackIds=null;
        Cell taskId=null;

        for(Cell kv : keyValues){
            if(CellUtils.singleMatchingColumn(kv,FAMILY,DATA_QUALIFIER_BYTES))
                dataKv=kv;
            else if(CellUtils.singleMatchingColumn(kv,FAMILY,KEEP_ALIVE_QUALIFIER_BYTES))
                keepAliveKv=kv;
            else if(CellUtils.singleMatchingColumn(kv,FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES))
                globalCommitKv=kv;
            else if(CellUtils.singleMatchingColumn(kv,FAMILY,COMMIT_QUALIFIER_BYTES))
                commitKv=kv;
            else if(CellUtils.singleMatchingColumn(kv,FAMILY,STATE_QUALIFIER_BYTES))
                stateKv=kv;
            else if(CellUtils.singleMatchingColumn(kv,FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES))
                destinationTables=kv;
            else if(CellUtils.singleMatchingColumn(kv,FAMILY,ROLLBACK_SUBTRANSACTIONS_QUALIFIER_BYTES))
                rollbackIds=kv;
            else if(CellUtils.singleMatchingColumn(kv,FAMILY,TASK_QUALIFIER_BYTES))
                taskId=kv;
        }
        if(dataKv==null) return null;

        long txnId=TxnUtils.txnIdFromRowKey(dataKv.getRowArray(),dataKv.getRowOffset(),dataKv.getRowLength());
        return decodeInternal(txnStore,dataKv,keepAliveKv,commitKv,globalCommitKv,stateKv,destinationTables,txnId, rollbackIds,taskId);
    }

    @Override
    public TxnMessage.TaskId decodeTaskId(RegionTxnStore txnStore,
                                 long txnId,Result result) throws IOException {
        Cell taskKv=result.getColumnLatestCell(FAMILY,TASK_QUALIFIER_BYTES);
        return decodeTaskId(taskKv);
    }


    public TxnMessage.TaskId decodeTaskId(Cell taskKv) {
        if(taskKv==null) return null;
        MultiFieldDecoder decoder=MultiFieldDecoder.wrap(taskKv.getValueArray(),taskKv.getValueOffset(),taskKv.getValueLength());
        decoder.decodeNextString(); // igored
        decoder.decodeNextInt(); // ignored
        int stageId=decoder.decodeNextInt();
        int partitionId=decoder.decodeNextInt();
        int attemptNumber=decoder.decodeNextInt();

        return TxnMessage.TaskId.newBuilder().setStageId(stageId).setPartitionId(partitionId).setTaskAttemptNumber(attemptNumber).build();
    }

    @Override
    public TxnMessage.Txn decode(RegionTxnStore txnStore,
                                 long txnId,Result result) throws IOException{
        Cell dataKv=result.getColumnLatestCell(FAMILY,DATA_QUALIFIER_BYTES);
        Cell commitTsVal=result.getColumnLatestCell(FAMILY,COMMIT_QUALIFIER_BYTES);
        Cell globalTsVal=result.getColumnLatestCell(FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES);
        Cell stateKv=result.getColumnLatestCell(FAMILY,STATE_QUALIFIER_BYTES);
        Cell destinationTables=result.getColumnLatestCell(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES);
        Cell kaTime=result.getColumnLatestCell(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES);
        Cell rollbackIds=result.getColumnLatestCell(FAMILY,ROLLBACK_SUBTRANSACTIONS_QUALIFIER_BYTES);
        Cell taskKv=result.getColumnLatestCell(FAMILY,TASK_QUALIFIER_BYTES);

        if(dataKv==null) return null;
        return decodeInternal(txnStore,dataKv,kaTime,commitTsVal,globalTsVal,stateKv,destinationTables,txnId,rollbackIds, taskKv);
    }

    protected long toLong(Cell data){
        return Encoding.decodeLong(data.getValueArray(),data.getValueOffset(),false);
    }

    /**
     * @param txnStore used to adjust state of a transaction changing it from ACTIVE to ROLLBACK if keepalive  period is exceeded.
     *                 if null, adjustment will not take place.
     */
    protected TxnMessage.Txn decodeInternal(RegionTxnStore txnStore,
                                            Cell dataKv, Cell keepAliveKv, Cell commitKv, Cell globalCommitKv,
                                            Cell stateKv, Cell destinationTables, long txnId, Cell rollbackIds, Cell taskKv){
        long subId = txnId & SIConstants.SUBTRANSANCTION_ID_MASK;

        MultiFieldDecoder decoder=MultiFieldDecoder.wrap(dataKv.getValueArray(),dataKv.getValueOffset(),dataKv.getValueLength());
        long beginTs=decoder.decodeNextLong();
        long parentTxnId=-1l;
        if(!decoder.nextIsNull()) parentTxnId=decoder.decodeNextLong();
        else decoder.skip();

        boolean isAdditive=false;
        boolean hasAdditive=true;
        if(!decoder.nextIsNull())
            isAdditive=decoder.decodeNextBoolean();
        else{
            hasAdditive=false;
            decoder.skip();
        }
        Txn.IsolationLevel level=null;
        if(!decoder.nextIsNull())
            level=Txn.IsolationLevel.fromByte(decoder.decodeNextByte());
        else decoder.skip();

        long commitTs=-1l;
        if(commitKv!=null)
            commitTs=toLong(commitKv);

        long globalTs=-1l;
        if(globalCommitKv!=null)
            globalTs=toLong(globalCommitKv);

        Txn.State state=Txn.State.decode(stateKv.getValueArray(),stateKv.getValueOffset(),stateKv.getValueLength());
        //adjust for committed timestamp
        if(commitTs>0 || globalTs>0){
            //we have a commit timestamp, our state MUST be committed
            state=Txn.State.COMMITTED;
        }

        if(state==Txn.State.ACTIVE && txnStore != null){
                                /*
								 * We need to check that the transaction hasn't been timed out (and therefore rolled back). This
								 * happens if the keepAliveTime is older than the configured transaction timeout. Of course,
								 * there is some network latency which could cause small keep alives to be problematic. To help out,
								 * we allow a little fudge factor in the timeout
								 */
            state=txnStore.adjustStateForTimeout(state,keepAliveKv);
        }
        long kaTime=decodeKeepAlive(keepAliveKv,false);
        List<Long> rollbackSubIds=decodeRollbackIds(rollbackIds);
        if (subId != 0 && rollbackSubIds.contains(subId)) {
            state = Txn.State.ROLLEDBACK;
        }
        TxnMessage.TaskId taskId = null;
        if (taskKv != null) {
            taskId = decodeTaskId(taskKv);
        }
        return composeValue(destinationTables,level,txnId,beginTs,parentTxnId,hasAdditive,
                isAdditive,commitTs,globalTs,state,kaTime,rollbackSubIds,taskId);

    }

    private List<Long> decodeRollbackIds(Cell rollbackIds) {
        if (rollbackIds == null)
            return Collections.emptyList();
        List<Long> ids = new ArrayList<>();
        MultiFieldDecoder decoder=MultiFieldDecoder.wrap(rollbackIds.getValueArray(), rollbackIds.getValueOffset(), rollbackIds.getValueLength());
        while (decoder.available()) {
            ids.add(decoder.decodeNextLong());
        }
        return ids;
    }

    protected static long decodeKeepAlive(Cell columnLatest,boolean oldForm){
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
            if(length==0) return 0l;
            else
                lastKATime=Bytes.toLong(columnLatest.getValueArray(),columnLatest.getValueOffset(),length);
        }else
            lastKATime=Encoding.decodeLong(columnLatest.getValueArray(),columnLatest.getValueOffset(),false);
        return lastKATime;
    }

    /*
	 * Encodes transaction objects using the new, packed Encoding format
	 *
	 * The new way is a (more) compact representation which uses the Values CF (V) and compact qualifiers (using
	 * the Encoding.encodeX() methods) as follows:
	 *
	 * "d"	--	packed tuple of (beginTimestamp,parentTxnId,isDependent,additive,isolationLevel)
	 * "c"	--	counter (using a packed integer representation)
	 * "k"	--	keepAlive timestamp
	 * "t"	--	commit timestamp
	 * "g"	--	globalCommitTimestamp
	 * "s"	--	state
	 * "v"  --  taskId
	 *
	 * The additional columns are kept separate so that they may be updated(and read) independently without
	 * reading and decoding the entire transaction.
	 *
	 * In the new format, if a transaction has been written to the table, then it automatically allows writes
	 *
	 * order: c,d,e,g,k,s,t,v
	 * order: counter,data,destinationTable,globalCommitTimestamp,keepAlive,state,commitTimestamp,taskId
	 */
    public org.apache.hadoop.hbase.client.Put encodeForPut(TxnMessage.TxnInfo txnInfo,byte[] rowKey) throws IOException{
        org.apache.hadoop.hbase.client.Put put=new org.apache.hadoop.hbase.client.Put(rowKey);
        MultiFieldEncoder metaFieldEncoder=MultiFieldEncoder.create(5);
        metaFieldEncoder.encodeNext(txnInfo.getBeginTs()).encodeNext(txnInfo.getParentTxnid());

        if(txnInfo.hasIsAdditive())
            metaFieldEncoder.encodeNext(txnInfo.getIsAdditive());
        else
            metaFieldEncoder.encodeEmpty();

        Txn.IsolationLevel level=Txn.IsolationLevel.fromInt(txnInfo.getIsolationLevel());
        if(level!=null)
            metaFieldEncoder.encodeNext(level.encode());
        else
            metaFieldEncoder.encodeEmpty();

        Txn.State state=Txn.State.ACTIVE;

        put.addColumn(FAMILY,DATA_QUALIFIER_BYTES,metaFieldEncoder.build());
        put.addColumn(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES,Encoding.encode(System.currentTimeMillis()));
        put.addColumn(FAMILY,STATE_QUALIFIER_BYTES,state.encode());
        ByteString destTableBuffer=txnInfo.getDestinationTables();
        if(destTableBuffer!=null && !destTableBuffer.isEmpty())
            put.addColumn(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES,destTableBuffer.toByteArray());
        if (txnInfo.hasTaskId()) {
            TxnMessage.TaskId taskId = txnInfo.getTaskId();
            MultiFieldEncoder taskIdEncoder=MultiFieldEncoder.create(5);
            taskIdEncoder.encodeNext("").encodeNext(0)
                    .encodeNext(taskId.getStageId()).encodeNext(taskId.getPartitionId()).encodeNext(taskId.getTaskAttemptNumber());
            put.addColumn(FAMILY,TASK_QUALIFIER_BYTES,taskIdEncoder.build());
        }
        return put;
    }

    protected TxnMessage.Txn composeValue(Cell destinationTables,
                                          Txn.IsolationLevel level, long txnId, long beginTs, long parentTs, boolean hasAdditive,
                                          boolean additive, long commitTs, long globalCommitTs, Txn.State state, long kaTime, List<Long> rollbackSubIds, TxnMessage.TaskId taskId){
        return TXNDecoderUtils.composeValue(destinationTables,level,txnId,beginTs,parentTs,hasAdditive,additive,commitTs,globalCommitTs,state,kaTime,rollbackSubIds,taskId);
    }

    @Override
    public TxnMessage.Txn decodeV1(RegionTxnStore txnStore, long txnId,Result result) throws IOException{
        Cell dataKv=result.getColumnLatestCell(FAMILY,DATA_QUALIFIER_BYTES);
        Cell commitTsVal=result.getColumnLatestCell(FAMILY,COMMIT_QUALIFIER_BYTES);
        Cell globalTsVal=result.getColumnLatestCell(FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES);
        Cell stateKv=result.getColumnLatestCell(FAMILY,STATE_QUALIFIER_BYTES);
        Cell destinationTables=result.getColumnLatestCell(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES);
        Cell kaTime=result.getColumnLatestCell(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES);
        if(dataKv==null) return null;
        return decodeInternalV1(txnStore,dataKv,kaTime,commitTsVal,globalTsVal,stateKv,destinationTables,txnId);
    }

    protected TxnMessage.Txn decodeInternalV1(RegionTxnStore txnStore, Cell dataKv,Cell keepAliveKv,Cell commitKv,Cell globalCommitKv, Cell stateKv,Cell destinationTables,long txnId){
        MultiFieldDecoder decoder=MultiFieldDecoder.wrap(dataKv.getValueArray(),dataKv.getValueOffset(),dataKv.getValueLength());
        long beginTs=decoder.decodeNextLong();
        long parentTxnId=-1l;
        if(!decoder.nextIsNull()) parentTxnId=decoder.decodeNextLong();
        else decoder.skip();
        boolean isAdditive=false;
        boolean hasAdditive=true;
        if(!decoder.nextIsNull())
            isAdditive=decoder.decodeNextBoolean();
        else{
            hasAdditive=false;
            decoder.skip();
        }
        Txn.IsolationLevel level=null;
        if(!decoder.nextIsNull())
            level=Txn.IsolationLevel.fromByte(decoder.decodeNextByte());
        else decoder.skip();
        long commitTs=-1l;
        if(commitKv!=null)
            commitTs=toLong(commitKv);
        long globalTs=-1l;
        if(globalCommitKv!=null)
            globalTs=toLong(globalCommitKv);
        Txn.State state=Txn.State.decode(stateKv.getValueArray(),stateKv.getValueOffset(),stateKv.getValueLength());
        //adjust for committed timestamp
        if(commitTs>0 || globalTs>0){
            //we have a commit timestamp, our state MUST be committed
            state=Txn.State.COMMITTED;
        }
        if(state==Txn.State.ACTIVE){
            /*
			 * We need to check that the transaction hasn't been timed out (and therefore rolled back). This
 			 * happens if the keepAliveTime is older than the configured transaction timeout. Of course,
 			 * there is some network latency which could cause small keep alives to be problematic. To help out,
 			 * we allow a little fudge factor in the timeout
 			 */
            state=txnStore.adjustStateForTimeout(state,keepAliveKv);
        }
        long kaTime=decodeKeepAlive(keepAliveKv,false);
        return composeValue(destinationTables,level,txnId,beginTs,parentTxnId,hasAdditive,
                isAdditive,commitTs,globalTs,state,kaTime,Collections.emptyList(), null);
    }

    public static Iterator<ByteSlice> decodeDestinationTables(ByteString array) {
        final Iterator<ByteSlice> destinationTables;
        if(array != null){
            MultiFieldDecoder dcd=MultiFieldDecoder.wrap(array.toByteArray());
            destinationTables=new DecodingIterator(dcd){
                @Override
                protected void advance(MultiFieldDecoder decoder){
                    decoder.skip();
                }
            };
        }else
            destinationTables= Iterators.emptyIterator();

        return new Iterator<ByteSlice>(){

            final Iterator<ByteSlice> it = destinationTables;

            @Override
            public boolean hasNext(){
                return it.hasNext();
            }

            @Override
            public ByteSlice next(){
                ByteSlice dSlice=it.next();
                byte[] data=Encoding.decodeBytesUnsortd(dSlice.array(),dSlice.offset(),dSlice.length());
                dSlice.set(data);
                return dSlice;
            }
        };
    }
}
