package com.splicemachine.si.impl;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.RegionScanner;


/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HTxnOperationFactory extends BaseOperationFactory<OperationWithAttributes,
        Cell,
        Delete,
        Get,
        Mutation,
        Put,
        RegionScanner,
        Result,
        Scan>{

    public HTxnOperationFactory(SDataLib<OperationWithAttributes, Cell, Delete, Get, Put, RegionScanner, Result, Scan> dataLib,
                                ExceptionFactory exceptionFactory){
        super(dataLib,exceptionFactory);
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
    public DataPut newDataPut(TxnView txn,byte[] key){
        DataPut put = new HPut(key);
        encodeForWrites(put,txn);
        return put;
    }

    @Override
    public DataDelete newDataDelete(TxnView txn,byte[] key){
        DataDelete delete = new HDelete(key);
        encodeForWrites(delete,txn);
        return delete;
    }
}
