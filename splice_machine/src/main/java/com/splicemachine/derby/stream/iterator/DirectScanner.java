package com.splicemachine.derby.stream.iterator;

import org.sparkproject.guava.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataScanner;
import java.io.IOException;
import java.util.List;

/**
 *
 * Created by jyuan on 10/16/15.
 */
public class DirectScanner implements StandardIterator<KVPair>, AutoCloseable{
    private DataScanner regionScanner;
    private List<DataCell> keyValues;

    public DirectScanner(DataScanner scanner){
        this.regionScanner=scanner;
    }

    @Override
    public void open() throws StandardException, IOException{

    }

    @Override
    public KVPair next() throws StandardException, IOException{
        if(keyValues==null)
            keyValues=Lists.newArrayListWithExpectedSize(2);
        keyValues.clear();
        keyValues=regionScanner.next(-1);//dataLib.regionScannerNext(regionScanner, keyValues);
        if(keyValues.size()<=0){
            return null;
        }else{
            DataCell currentKeyValue=keyValues.get(0);
            return getKVPair(currentKeyValue);
        }
    }

    @Override
    public void close() throws StandardException, IOException{
        if(regionScanner!=null)
            regionScanner.close();
    }

    private KVPair getKVPair(DataCell keyValue){
        int keyLen=keyValue.keyLength();
        int valueLen=keyValue.valueLength();
        byte[] key=new byte[keyLen];
        byte[] value=new byte[valueLen];
        System.arraycopy(keyValue.keyArray(),keyValue.keyOffset(),key,0,keyLen);
        System.arraycopy(keyValue.valueArray(),keyValue.valueOffset(),value,0,valueLen);
        return new KVPair(key,value);
    }
}
