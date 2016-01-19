package com.splicemachine.si.impl;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;

import java.io.IOException;


/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HTxnOperationFactory extends SimpleTxnOperationFactory{

    public HTxnOperationFactory(ExceptionFactory exceptionFactory){
        super(exceptionFactory);
    }

    @Override
    public DataScan newDataScan(TxnView txn){
        DataScan scan = new HScan();
        encodeForReads(scan,txn,false);
        return scan;
    }

    @Override
    public DataGet newDataGet(TxnView txn,byte[] rowKey,DataGet previous){
        DataGet get;
        if(previous!=null){
            ((HGet)previous).reset(rowKey);
            get = previous;
        }else
            get = new HGet(rowKey);
        encodeForReads(get,txn,false);
        return get;
    }

    @Override
    public DataPut newDataPut(TxnView txn,byte[] key) throws IOException{
        DataPut put = new HPut(key);
        if(txn!=null)
            encodeForWrites(put,txn);
        return put;
    }

    @Override
    public DataMutation newDataDelete(TxnView txn,byte[] key) throws IOException{
        if(txn==null){
            return new HDelete(key);
        }
        HPut put = new HPut(key);
        put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,txn.getTxnId(),SIConstants.EMPTY_BYTE_ARRAY);
        put.addAttribute(SIConstants.SI_DELETE_PUT,SIConstants.EMPTY_BYTE_ARRAY);
        encodeForWrites(put,txn);
        return put;
    }
}
