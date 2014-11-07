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
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.DenseTxn;
import com.splicemachine.si.impl.SparseTxn;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.List;

public class V2TxnDecoder<Data,Put extends OperationWithAttributes,Delete,Get extends OperationWithAttributes, Scan> extends TxnDecoder<Data,Put,Delete,Get,Scan>{
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

    public org.apache.hadoop.hbase.client.Put encodeForPut(SparseTxn txn) throws IOException {
    	org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(TxnUtils.getRowKey(txn.getTxnId()));
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
    public DenseTxn decode(SDataLib<Data,Put,Delete,Get,Scan> dataLib,List<Data> keyValues) throws IOException {
        if(keyValues.size()<=0) return null;
        Data dataKv = null;
        Data keepAliveKv = null;
        Data commitKv = null;
        Data globalCommitKv = null;
        Data stateKv = null;
        Data destinationTables = null;

        for(Data kv: keyValues){
            if(dataLib.singleMatchingColumn(kv, FAMILY, DATA_QUALIFIER_BYTES))
                dataKv = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,KEEP_ALIVE_QUALIFIER_BYTES))
                keepAliveKv = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES))
                globalCommitKv = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,COMMIT_QUALIFIER_BYTES))
                commitKv = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,STATE_QUALIFIER_BYTES))
                stateKv = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES))
                destinationTables = kv;
        }
        if(dataKv==null) return null;

        long txnId = TxnUtils.txnIdFromRowKey(dataLib.getDataRowBuffer(dataKv),dataLib.getDataRowOffset(dataKv),dataLib.getDataRowlength(dataKv));

        return decodeInternal(dataLib,dataKv, keepAliveKv, commitKv, globalCommitKv, stateKv, destinationTables, txnId);
    }

    @Override
    public SparseTxn decode(SDataLib<Data,Put,Delete,Get,Scan> dataLib, long txnId, Result result) throws IOException {
        Data dataKv = dataLib.getColumnLatest(result,FAMILY, DATA_QUALIFIER_BYTES);
        Data commitTsVal = dataLib.getColumnLatest(result,FAMILY,COMMIT_QUALIFIER_BYTES);
        Data globalTsVal = dataLib.getColumnLatest(result,FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES);
        Data stateKv = dataLib.getColumnLatest(result,FAMILY,STATE_QUALIFIER_BYTES);
        Data destinationTables = dataLib.getColumnLatest(result,FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES);
        Data kaTime = dataLib.getColumnLatest(result,FAMILY, KEEP_ALIVE_QUALIFIER_BYTES);

        if(dataKv==null) return null;
        return decodeInternal(dataLib,dataKv,kaTime,commitTsVal,globalTsVal,stateKv,destinationTables,txnId);
    }

    protected long toLong(SDataLib<Data,Put,Delete,Get,Scan> dataLib, Data data) {
    	return Encoding.decodeLong(dataLib.getDataValueBuffer(data), dataLib.getDataValueOffset(data), false);
    }
    
    protected DenseTxn decodeInternal(SDataLib<Data,Put,Delete,Get,Scan> dataLib, Data dataKv, Data keepAliveKv, Data commitKv, Data globalCommitKv, Data stateKv, Data destinationTables, long txnId) {
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(dataLib.getDataValueBuffer(dataKv), 
        		dataLib.getDataValueOffset(dataKv), dataLib.getDataValuelength(dataKv));
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
            commitTs = toLong(dataLib,commitKv);

        long globalTs = -1l;
        if(globalCommitKv!=null)
            globalTs = toLong(dataLib,globalCommitKv);

        Txn.State state = Txn.State.decode(dataLib.getDataValueBuffer(stateKv),
        		dataLib.getDataValueOffset(stateKv),dataLib.getDataValuelength(stateKv));
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
            state = adjustStateForTimeout(dataLib,state, keepAliveKv,false);
        }
        long kaTime = decodeKeepAlive(dataLib,keepAliveKv,false);

        ByteSlice destTableBuffer = null;
        if(destinationTables!=null){
            destTableBuffer = new ByteSlice();
            destTableBuffer.set(dataLib.getDataValueBuffer(destinationTables),dataLib.getDataValueOffset(destinationTables),dataLib.getDataValuelength(destinationTables));
        }

        return new DenseTxn(txnId,beginTs,parentTxnId,
                commitTs,globalTs, hasAdditive,isAdditive,level,state,destTableBuffer,kaTime);
    }



}
