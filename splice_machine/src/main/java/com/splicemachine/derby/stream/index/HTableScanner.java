package com.splicemachine.derby.stream.index;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.txn.DDLTxnView;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;
import com.splicemachine.storage.DataScanner;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 *
 * Created by jyuan on 10/16/15.
 */
public class HTableScanner implements StandardIterator<KVPair>, AutoCloseable{
    private DataScanner regionScanner;
    private final TransactionalRegion region;
    private final Counter filterCounter;
    private TxnFilter txnFilter;
    private List<DataCell> keyValues;
    private TxnView txn;
    private long demarcationPoint;

    protected HTableScanner(DataScanner scanner,
                            final TransactionalRegion region,
                            final TxnView txn,
                            final long demarcationPoint,
                            MetricFactory metricFactory){
        this.region=region;
        this.regionScanner=scanner;
        this.txn=txn;
        this.demarcationPoint=demarcationPoint;
        this.filterCounter=metricFactory.newCounter();
//        BitSet indexedColumns=new BitSet();
//        for(int indexCol:indexColToMainColPosMap){
//            indexedColumns.set(indexCol-1);
//        }
    }

    @Override
    public void open() throws StandardException, IOException{

    }

    @Override
    public KVPair next() throws StandardException, IOException{
        TxnFilter filter=getTxnFilter();
        if(keyValues==null)
            keyValues=Lists.newArrayListWithExpectedSize(2);
        do{
            keyValues.clear();
            keyValues=regionScanner.next(-1);//dataLib.regionScannerNext(regionScanner, keyValues);
            if(keyValues.size()<=0){
                return null;
            }else{
                if(!filterRow(filter)){
                    filterCounter.increment();
                    continue;
                }
                DataCell currentKeyValue=keyValues.get(0);
                return getKVPair(currentKeyValue);
            }
        }while(true); //this is weird
    }

    @Override
    public void close() throws StandardException, IOException{
        if(regionScanner!=null)
            regionScanner.close();
    }

    public long getRowsFiltered(){
        return filterCounter.getTotal();
    }

    private TxnFilter getTxnFilter() throws IOException{
        if(txnFilter==null){
            DDLTxnView demarcationTxn=new DDLTxnView(txn,this.demarcationPoint);
            txnFilter=region.unpackedFilter(demarcationTxn);
        }
        return txnFilter;
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

    private boolean filterRow(TxnFilter filter) throws IOException{
        Iterator<DataCell> kvIter=keyValues.iterator();
        while(kvIter.hasNext()){
            DataCell kv=kvIter.next();
            final DataFilter.ReturnCode returnCode=filter.filterCell(kv);
            switch(kv.dataType()){
                case USER_DATA:
                    switch(returnCode){
                        case INCLUDE:
                        case INCLUDE_AND_NEXT_COL:
                            return true;
                        case SKIP:
                        case NEXT_COL:
                        case NEXT_ROW:
                            kvIter.remove();
                            break;
                        default:
                            throw new RuntimeException("unknown return code");
                    }
                    break;
                case COMMIT_TIMESTAMP:
                case TOMBSTONE:
                case ANTI_TOMBSTONE:
                case FOREIGN_KEY_COUNTER:
                case OTHER:
                    kvIter.remove();
                    break;
                default:
                    throw new RuntimeException("unknown key value type");

            }
        }
        return keyValues.size()>0;
    }
}
