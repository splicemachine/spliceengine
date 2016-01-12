package com.splicemachine.si.impl;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.*;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MTxnOperationFactory extends BaseOperationFactory{
    public MTxnOperationFactory(SDataLib dataLib,ExceptionFactory exceptionFactory){
        super(dataLib,exceptionFactory);
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
    public DataDelete newDataDelete(TxnView txn,byte[] key) throws IOException{
        DataDelete delete = new MDelete(key);
        encodeForWrites(delete,txn);
        return delete;
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
