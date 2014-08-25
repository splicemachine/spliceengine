package com.splicemachine.si.impl.region;

/**
 * Decoder which decodes Transactions stored in the
 * @author Scott Fines
 * Date: 8/18/14
 */

import com.splicemachine.constants.SIConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.DenseTxn;
import com.splicemachine.si.impl.SparseTxn;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

class V2TxnDecoder extends TxnDecoder{
    private static final byte[] FAMILY = SIConstants.DEFAULT_FAMILY_BYTES;
    static final byte[] DATA_QUALIFIER_BYTES = Bytes.toBytes("d");
    static final byte[] COUNTER_QUALIFIER_BYTES = Bytes.toBytes("c");
    static final byte[] KEEP_ALIVE_QUALIFIER_BYTES = Bytes.toBytes("k");
    static final byte[] COMMIT_QUALIFIER_BYTES = Bytes.toBytes("t");
    static final byte[] GLOBAL_COMMIT_QUALIFIER_BYTES = Bytes.toBytes("g");
    static final byte[] STATE_QUALIFIER_BYTES = Bytes.toBytes("s");
    static final byte[] DESTINATION_TABLE_QUALIFIER_BYTES = Bytes.toBytes("e"); //had to pick a letter that was unique

    public static final V2TxnDecoder INSTANCE = new V2TxnDecoder();

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
				 *
				 * The additional columns are kept separate so that they may be updated(and read) independently without
				 * reading and decoding the entire transaction.
				 *
				 * In the new format, if a transaction has been written to the table, then it automatically allows writes
				 *
				 * order: c,d,e,g,k,s,t
				 * order: counter,data,destinationTable,globalCommitTimestamp,keepAlive,state,commitTimestamp,
				 */

    public Put encodeForPut(SparseTxn txn) throws IOException {
        Put put = new Put(TxnUtils.getRowKey(txn.getTxnId()));
        MultiFieldEncoder metaFieldEncoder = MultiFieldEncoder.create(5);
        metaFieldEncoder.encodeNext(txn.getBeginTimestamp()).encodeNext(txn.getParentTxnId());

        if(txn.hasAdditiveField())
            metaFieldEncoder.encodeNext(txn.isAdditive());
        else
            metaFieldEncoder.encodeEmpty();

        Txn.IsolationLevel level = txn.getIsolationLevel();
        if(level!=null)
            metaFieldEncoder.encodeNext(level.encode());
        else metaFieldEncoder.encodeEmpty();

        put.add(FAMILY,DATA_QUALIFIER_BYTES,metaFieldEncoder.build());
        put.add(FAMILY,COUNTER_QUALIFIER_BYTES, Encoding.encode(0l));
        put.add(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES,Encoding.encode(System.currentTimeMillis()));
        put.add(FAMILY,STATE_QUALIFIER_BYTES,txn.getState().encode());
        if(txn.getState()== Txn.State.COMMITTED){
            put.add(FAMILY,COMMIT_QUALIFIER_BYTES,Encoding.encode(txn.getCommitTimestamp()));
            long globalCommitTs = txn.getGlobalCommitTimestamp();
            if(globalCommitTs>=0)
                put.add(FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES,Encoding.encode(globalCommitTs));
        }
        ByteSlice destTableBuffer = txn.getDestinationTableBuffer();
        if(destTableBuffer!=null && destTableBuffer.length()>0)
            put.add(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES,destTableBuffer.getByteCopy());
        return put;
    }

    @Override
    public DenseTxn decode(List<KeyValue> keyValues) throws IOException {
        if(keyValues.size()<=0) return null;
        KeyValue dataKv = null;
        KeyValue keepAliveKv = null;
        KeyValue commitKv = null;
        KeyValue globalCommitKv = null;
        KeyValue stateKv = null;
        KeyValue destinationTables = null;

        for(KeyValue kv: keyValues){
            if(KeyValueUtils.singleMatchingColumn(kv, FAMILY, DATA_QUALIFIER_BYTES))
                dataKv = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,KEEP_ALIVE_QUALIFIER_BYTES))
                keepAliveKv = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES))
                globalCommitKv = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,COMMIT_QUALIFIER_BYTES))
                commitKv = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,STATE_QUALIFIER_BYTES))
                stateKv = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES))
                destinationTables = kv;
        }
        if(dataKv==null) return null;

        long txnId = TxnUtils.txnIdFromRowKey(dataKv.getBuffer(),dataKv.getRowOffset(),dataKv.getRowLength());

        return decodeInternal(dataKv, keepAliveKv, commitKv, globalCommitKv, stateKv, destinationTables, txnId);
    }

    @Override
    public SparseTxn decode(long txnId, Result result) throws IOException {
        KeyValue dataKv = result.getColumnLatest(FAMILY, DATA_QUALIFIER_BYTES);
        KeyValue commitTsVal = result.getColumnLatest(FAMILY,COMMIT_QUALIFIER_BYTES);
        KeyValue globalTsVal = result.getColumnLatest(FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES);
        KeyValue stateKv = result.getColumnLatest(FAMILY,STATE_QUALIFIER_BYTES);
        KeyValue destinationTables = result.getColumnLatest(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES);
        KeyValue kaTime = result.getColumnLatest(FAMILY, KEEP_ALIVE_QUALIFIER_BYTES);

        if(dataKv==null) return null;
        return decodeInternal(dataKv,kaTime,commitTsVal,globalTsVal,stateKv,destinationTables,txnId);
    }

    protected DenseTxn decodeInternal(KeyValue dataKv, KeyValue keepAliveKv, KeyValue commitKv, KeyValue globalCommitKv, KeyValue stateKv, KeyValue destinationTables, long txnId) {
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(dataKv.getBuffer(), dataKv.getValueOffset(), dataKv.getValueLength());
        long beginTs = decoder.decodeNextLong();
        long parentTxnId = -1l;
        if(!decoder.nextIsNull()) parentTxnId = decoder.decodeNextLong();
        else decoder.skip();

        boolean isAdditive = false;
        boolean hasAdditive = true;
        if(!decoder.nextIsNull())
            isAdditive = decoder.decodeNextBoolean();
        else {
            hasAdditive = false;
            decoder.skip();
        }
        Txn.IsolationLevel level = null;
        if(!decoder.nextIsNull())
            level = Txn.IsolationLevel.fromByte(decoder.decodeNextByte());
        else decoder.skip();

        long commitTs = -1l;
        if(commitKv!=null)
            commitTs = Encoding.decodeLong(commitKv.getBuffer(), commitKv.getValueOffset(), false);

        long globalTs = -1l;
        if(globalCommitKv!=null)
            globalTs = Encoding.decodeLong(globalCommitKv.getBuffer(), globalCommitKv.getValueOffset(), false);

        Txn.State state = Txn.State.decode(stateKv.getBuffer(),stateKv.getValueOffset(),stateKv.getValueLength());
        //adjust for committed timestamp
        if(commitTs>0 || globalTs>0){
            //we have a commit timestamp, our state MUST be committed
            state = Txn.State.COMMITTED;
        }

        if(state== Txn.State.ACTIVE){
								/*
								 * We need to check that the transaction hasn't been timed out (and therefore rolled back). This
								 * happens if the keepAliveTime is older than the configured transaction timeout. Of course,
								 * there is some network latency which could cause small keep alives to be problematic. To help out,
								 * we allow a little fudge factor in the timeout
								 */
            state = adjustStateForTimeout(state, keepAliveKv,false);
        }
        long kaTime = decodeKeepAlive(keepAliveKv,false);

        ByteSlice destTableBuffer = null;
        if(destinationTables!=null){
            destTableBuffer = new ByteSlice();
            destTableBuffer.set(destinationTables.getBuffer(),destinationTables.getValueOffset(),destinationTables.getValueLength());
        }

        return new DenseTxn(txnId,beginTs,parentTxnId,
                commitTs,globalTs, hasAdditive,isAdditive,level,state,destTableBuffer,kaTime);
    }



}
