package com.splicemachine.si.impl;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MTxnOperationFactory extends BaseOperationFactory{
    private Clock clock;

    public MTxnOperationFactory(SDataLib dataLib,Clock clock,ExceptionFactory exceptionFactory){
        super(dataLib,exceptionFactory);
        this.clock = clock;
    }

    @Override
    public void writeScan(DataScan scan,ObjectOutput out) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataScan readScan(ObjectInput in) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataGet newDataGet(TxnView txn,byte[] rowKey,DataGet previous){
        MGet get = new MGet(rowKey);
        encodeForReads(get,txn,false);
        return get;
    }

    @Override
    public DataPut newDataPut(TxnView txn,byte[] key) throws IOException{
        DataPut dp = new MPut(key);
        if(txn==null){
            makeNonTransactional(dp);
        }else
            encodeForWrites(dp,txn);
        return dp;
    }

    @Override
    public DataMutation newDataDelete(TxnView txn,byte[] key) throws IOException{
        if(txn==null){
            return new MDelete(key);
        }
        MPut put = new MPut(key);
        put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,txn.getTxnId(),SIConstants.EMPTY_BYTE_ARRAY);
        put.addAttribute(SIConstants.SI_DELETE_PUT,SIConstants.TRUE_BYTES);
        encodeForWrites(put,txn);
        return put;
    }

    @Override
    public DataCell newDataCell(byte[] key,byte[] family,byte[] qualifier,byte[] value){
        return new MCell(key,family,qualifier,clock.currentTimeMillis(),value,CellType.USER_DATA);
    }

    @Override
    public DataScan newDataScan(TxnView txn){
        MScan scan = new MScan();
        if(txn!=null)
            encodeForReads(scan,txn,false);
        else
            makeNonTransactional(scan);
        return scan;
    }

}
