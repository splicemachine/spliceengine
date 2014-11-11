package com.splicemachine.si.api;

import org.apache.hadoop.hbase.regionserver.HRegion;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.region.STransactionLib;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;

public interface SIFactory {
	public RowAccumulator getRowAccumulator(EntryPredicateFilter predicateFilter, EntryDecoder decoder, boolean countStar);
	public RowAccumulator getRowAccumulator(EntryPredicateFilter predicateFilter, EntryDecoder decoder, EntryAccumulator accumulator, boolean countStar);
    public STableWriter getTableWriter();
    public SDataLib getDataLib();
    public STransactionLib getTransactionLib();
    public DataStore getDataStore();
    public STableReader getTableReader();
    public TxnStore getTxnStore();
    public TxnSupplier getTxnSupplier();	
    public TransactionalRegion getTransactionalRegion(HRegion region);
}
