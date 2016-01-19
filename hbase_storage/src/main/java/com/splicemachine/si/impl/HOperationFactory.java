package com.splicemachine.si.impl;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.storage.*;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 1/18/16
 */
public class HOperationFactory implements OperationFactory{
    @Override
    public DataScan newScan(){
        return new HScan();
    }

    @Override
    public DataGet newGet(byte[] rowKey,DataGet previous){
        if(previous!=null){
            ((HGet)previous).reset(rowKey);
            return previous;
        }else
            return new HGet(rowKey);
    }

    @Override
    public DataPut newPut(byte[] rowKey){
        return new HPut(rowKey);
    }

    @Override
    public DataDelete newDelete(byte[] rowKey){
        return new HDelete(rowKey);
    }

    @Override
    public DataCell newCell(byte[] key,byte[] family,byte[] qualifier,byte[] value){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataCell newCell(byte[] key,byte[] family,byte[] qualifier,long timestamp,byte[] value){
        throw new UnsupportedOperationException("IMPLEMENT");
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
    public DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp){
        return null;
    }
}
