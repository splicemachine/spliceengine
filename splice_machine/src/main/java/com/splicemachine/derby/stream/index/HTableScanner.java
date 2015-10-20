package com.splicemachine.derby.stream.index;

import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.carrotsearch.hppc.BitSet;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.MeasuredRegionScanner;

import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.DDLTxnView;
import com.splicemachine.si.impl.TxnFilter;
import com.splicemachine.storage.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;
import com.splicemachine.derby.utils.StandardIterator;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jyuan on 10/16/15.
 */
public class HTableScanner<Data> implements StandardIterator<KVPair>,AutoCloseable{
    private static Logger LOG = Logger.getLogger(HTableScanner.class);
    private MeasuredRegionScanner<Data> regionScanner;
    private final TransactionalRegion region;
    private final Counter filterCounter;
    private final Scan scan;
    private final String tableVersion;
    private TxnFilter<Data> txnFilter;
    private EntryPredicateFilter predicateFilter;
    private List<Data> keyValues;
    private final SDataLib dataLib;
    private TxnView txn;
    private long demarcationPoint;
    private int[] indexColToMainColPosMap;
    private BitSet indexedColumns;

    protected HTableScanner(final SDataLib dataLib,
                            MeasuredRegionScanner<Data> scanner,
                            final TransactionalRegion region,
                            Scan scan,
                            final TxnView txn,
                            final long demarcationPoint,
                            final int[] indexColToMainColPosMap,
                            final String tableVersion,
                            MetricFactory metricFactory) {
        this.dataLib = dataLib;
        this.region = region;
        this.scan = scan;
        this.regionScanner = scanner;
        this.tableVersion = tableVersion;
        this.txn = txn;
        this.demarcationPoint = demarcationPoint;
        this.filterCounter = metricFactory.newCounter();
        this.indexColToMainColPosMap = indexColToMainColPosMap;
        indexedColumns = new BitSet();
        for(int indexCol:indexColToMainColPosMap){
            indexedColumns.set(indexCol-1);
        }
    }

    @Override
    public void open() throws StandardException, IOException {

    }

    @Override
    public KVPair next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        TxnFilter filter = getTxnFilter();
        if(keyValues==null)
            keyValues = Lists.newArrayListWithExpectedSize(2);
        boolean hasRow;
        do{
            keyValues.clear();
            hasRow = dataLib.regionScannerNext(regionScanner, keyValues);
            if(keyValues.size()<=0){
                return null;
            }else{
                    if(!filterRow(filter)){
                        filterCounter.increment();
                        continue;
                    }
                Data currentKeyValue = keyValues.get(0);
                return getKVPair(currentKeyValue);
            }
        }while(hasRow);
        return null;
    }

    @Override
    public void close() throws StandardException, IOException {
        if (regionScanner != null)
            regionScanner.close();
    }

    public long getRowsFiltered() {
        return filterCounter.getTotal();
    }

    private TxnFilter getTxnFilter() throws IOException {
        if (txnFilter == null) {
            DDLTxnView demarcationTxn=new DDLTxnView(txn,this.demarcationPoint);
            txnFilter = region.unpackedFilter(demarcationTxn);
        }
        return txnFilter;
    }

    private KVPair getKVPair(Data keyValue) {
        int keyLen = dataLib.getDataRowlength(keyValue);
        int valueLen = dataLib.getDataValuelength(keyValue);
        byte[] key = new byte[keyLen];
        byte[] value = new byte[valueLen];

        System.arraycopy(dataLib.getDataRowBuffer(keyValue), dataLib.getDataRowOffset(keyValue),
                key, 0, keyLen);

        System.arraycopy(dataLib.getDataValueBuffer(keyValue), dataLib.getDataValueOffset(keyValue),
                value, 0, valueLen);
        KVPair kvPair = new KVPair(key, value);
        return  kvPair;
    }

    private boolean filterRow (TxnFilter filter) throws IOException{
        Iterator<Data> kvIter = keyValues.iterator();
        while(kvIter.hasNext()){
            Data kv = kvIter.next();
            final Filter.ReturnCode returnCode = filter.filterKeyValue(kv);
            switch (filter.getType(kv)) {
                case USER_DATA:
                    switch (returnCode) {
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
        return keyValues.size() > 0;
    }
}
