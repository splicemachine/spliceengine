package com.splicemachine.si.impl.region;

/**
 * Decoder which decodes Transactions stored in the
 * @author Scott Fines
 * Date: 8/18/14
 */

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnDecoder;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.TxnUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;

public abstract class AbstractV2TxnDecoder<OperationWithAttributes, Delete extends OperationWithAttributes,Filter,Get extends OperationWithAttributes,
        Put extends OperationWithAttributes,RegionScanner,Scan extends OperationWithAttributes> implements TxnDecoder<OperationWithAttributes,Cell,Delete,Filter,
        Get,Put,RegionScanner,Result, Scan> {
    public static final byte[] FAMILY = SIConstants.DEFAULT_FAMILY_BYTES;
    public static final byte[] DATA_QUALIFIER_BYTES = Bytes.toBytes("d");
//    public static final byte[] COUNTER_QUALIFIER_BYTES = Bytes.toBytes("c");
    public static final byte[] KEEP_ALIVE_QUALIFIER_BYTES = Bytes.toBytes("k");
    public static final byte[] COMMIT_QUALIFIER_BYTES = Bytes.toBytes("t");
    public static final byte[] GLOBAL_COMMIT_QUALIFIER_BYTES = Bytes.toBytes("g");
    public static final byte[] STATE_QUALIFIER_BYTES = Bytes.toBytes("s");
    public static final byte[] DESTINATION_TABLE_QUALIFIER_BYTES = Bytes.toBytes("e"); //had to pick a letter that was unique
    public static long TRANSACTION_TIMEOUT = SIDriver.getSIFactory().getTransactionTimeout();
    private static final long TRANSACTION_TIMEOUT_WINDOW = TRANSACTION_TIMEOUT+1000;


    @Override
    public TxnMessage.Txn decode(SDataLib<OperationWithAttributes,Cell,Delete,Filter,Get,
            Put,RegionScanner,Result,Scan> dataLib, List<Cell> keyValues) throws IOException {
        if(keyValues.size()<=0) return null;
        Cell dataKv = null;
        Cell keepAliveKv = null;
        Cell commitKv = null;
        Cell globalCommitKv = null;
        Cell stateKv = null;
        Cell destinationTables = null;

        for(Cell kv: keyValues){
            if(dataLib.singleMatchingColumn(kv,FAMILY,DATA_QUALIFIER_BYTES))
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
    public TxnMessage.Txn decode(SDataLib<OperationWithAttributes,Cell,Delete,Filter,Get,
            Put,RegionScanner,Result,Scan> dataLib, long txnId, Result result) throws IOException {
    	Cell dataKv = result.getColumnLatestCell(FAMILY,DATA_QUALIFIER_BYTES);
        Cell commitTsVal = result.getColumnLatestCell(FAMILY,COMMIT_QUALIFIER_BYTES);
        Cell globalTsVal = result.getColumnLatestCell(FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES);
        Cell stateKv = result.getColumnLatestCell(FAMILY,STATE_QUALIFIER_BYTES);
        Cell destinationTables = result.getColumnLatestCell(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES);
        Cell kaTime = result.getColumnLatestCell(FAMILY, KEEP_ALIVE_QUALIFIER_BYTES);

        if(dataKv==null) return null;
        return decodeInternal(dataLib,dataKv,kaTime,commitTsVal,globalTsVal,stateKv,destinationTables,txnId);
    }

    protected long toLong(SDataLib<OperationWithAttributes,Cell,Delete,Filter,Get,
            Put,RegionScanner,Result,Scan> dataLib, Cell data) {
    	return Encoding.decodeLong(dataLib.getDataValueBuffer(data), dataLib.getDataValueOffset(data), false);
    }
        
    protected TxnMessage.Txn decodeInternal(SDataLib<OperationWithAttributes,Cell,Delete,Filter,Get,
            Put,RegionScanner,Result,Scan> dataLib,
                                         Cell dataKv, Cell keepAliveKv, Cell commitKv, Cell globalCommitKv,
                                         Cell stateKv, Cell destinationTables, long txnId) {
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
            state = adjustStateForTimeout(state, keepAliveKv,false);
        }
        long kaTime = decodeKeepAlive(keepAliveKv,false);
        return composeValue(destinationTables,level,txnId, beginTs,parentTxnId,hasAdditive,
        		isAdditive,commitTs,globalTs,state,kaTime);
        
    }

    protected static Txn.State adjustStateForTimeout(Txn.State currentState,Cell columnLatest,long currTime,boolean oldForm) {
        long lastKATime = decodeKeepAlive(columnLatest, oldForm);

        if((currTime-lastKATime)>TRANSACTION_TIMEOUT_WINDOW)
            return Txn.State.ROLLEDBACK; //time out the txn

        return currentState;
    }

    protected static  long decodeKeepAlive(Cell columnLatest,boolean oldForm) {
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
            int length = columnLatest.getValueLength();
            if(length==0) return 0l;
            else
                lastKATime = Bytes.toLong(columnLatest.getValueArray(), columnLatest.getValueOffset(), length);
        }else
            lastKATime = Encoding.decodeLong(columnLatest.getValueArray(), columnLatest.getValueOffset(), false);
        return lastKATime;
    }

    protected static  Txn.State adjustStateForTimeout(Txn.State currentState,Cell columnLatest,boolean oldForm) {
        return adjustStateForTimeout(currentState,columnLatest,System.currentTimeMillis(),oldForm);
    }



}
