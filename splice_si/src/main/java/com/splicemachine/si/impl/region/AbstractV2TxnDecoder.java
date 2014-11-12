package com.splicemachine.si.impl.region;

/**
 * Decoder which decodes Transactions stored in the
 * @author Scott Fines
 * Date: 8/18/14
 */

import com.splicemachine.constants.SIConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.TxnUtils;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public abstract class AbstractV2TxnDecoder<Transaction,Data,Put extends OperationWithAttributes,Delete,Get extends OperationWithAttributes, Scan> extends TxnDecoder<Transaction,Data,Put,Delete,Get,Scan>{
    public static final byte[] FAMILY = SIConstants.DEFAULT_FAMILY_BYTES;
    public static final byte[] DATA_QUALIFIER_BYTES = Bytes.toBytes("d");
    public static final byte[] COUNTER_QUALIFIER_BYTES = Bytes.toBytes("c");
    public static final byte[] KEEP_ALIVE_QUALIFIER_BYTES = Bytes.toBytes("k");
    public static final byte[] COMMIT_QUALIFIER_BYTES = Bytes.toBytes("t");
    public static final byte[] GLOBAL_COMMIT_QUALIFIER_BYTES = Bytes.toBytes("g");
    public static final byte[] STATE_QUALIFIER_BYTES = Bytes.toBytes("s");
    public static final byte[] DESTINATION_TABLE_QUALIFIER_BYTES = Bytes.toBytes("e"); //had to pick a letter that was unique

    @Override
    public Transaction decode(SDataLib<Data,Put,Delete,Get,Scan> dataLib,List<Data> keyValues) throws IOException {
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
    public Transaction decode(SDataLib<Data,Put,Delete,Get,Scan> dataLib, long txnId, Result result) throws IOException {
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
        
    protected Transaction decodeInternal(SDataLib<Data,Put,Delete,Get,Scan> dataLib, Data dataKv, Data keepAliveKv, Data commitKv, Data globalCommitKv, Data stateKv, Data destinationTables, long txnId) {
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
        return composeValue(destinationTables,level,txnId, beginTs,parentTxnId,hasAdditive,
        		isAdditive,commitTs,globalTs,state,kaTime);
        
    }

}
