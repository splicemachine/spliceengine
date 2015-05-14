package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.ExecRowAccumulator;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.PackedTxnFilter;
import com.splicemachine.si.impl.TxnFilter;
import com.splicemachine.storage.*;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * TableScanner which applies SI to generate a row
 * @author Scott Fines
 * Date: 4/4/14
 */
public class SITableScanner<Data> implements StandardIterator<ExecRow>,AutoCloseable{
    private static Logger LOG = Logger.getLogger(SITableScanner.class);
//    private final Timer timer;
//    private final Counter filterCounter;
    private MeasuredRegionScanner<Data> regionScanner;
    private final TransactionalRegion region;
    private final Scan scan;
    private ScopedPredicates<Data> scopedPredicates;
    private final ExecRow template;
    private final String tableVersion;
    private final int[] rowDecodingMap;
    private SIFilter<Data> siFilter;
    private EntryPredicateFilter predicateFilter;
    private List<Data> keyValues;
    private RowLocation currentRowLocation;
    private final boolean[] keyColumnSortOrder;
    private String indexName;
    private ByteSlice slice = new ByteSlice();
    private boolean isKeyed = true;
    private KeyIndex primaryKeyIndex;
    private MultiFieldDecoder keyDecoder;
    private final Supplier<MultiFieldDecoder> keyDecoderProvider;
    private ExecRowAccumulator keyAccumulator;
    private int[] keyDecodingMap;
    private FormatableBitSet accessedKeys;
    private final SIFilterFactory filterFactory;
    private ExecRowAccumulator accumulator;
    private final SDataLib dataLib;
    private EntryDecoder entryDecoder;


    protected SITableScanner(final SDataLib dataLib, MeasuredRegionScanner<Data> scanner,
                             final TransactionalRegion region,
                             final ExecRow template,
                             MetricFactory metricFactory,
                             Scan scan,
                             final int[] rowDecodingMap,
                             final TxnView txn,
                             int[] keyColumnEncodingOrder,
                             boolean[] keyColumnSortOrder,
                             int[] keyColumnTypes,
                             int[] keyDecodingMap,
                             FormatableBitSet accessedPks,
                             String indexName,
                             final String tableVersion,
                             SIFilterFactory filterFactory) {
        this.dataLib = dataLib;
        this.region = region;
        this.scan = scan;
        this.template = template;
        this.rowDecodingMap = rowDecodingMap;
        this.keyColumnSortOrder = keyColumnSortOrder;
        this.indexName = indexName;
//        this.filterCounter = metricFactory.newCounter();
        this.regionScanner = scanner;
        this.keyDecodingMap = keyDecodingMap;
        this.accessedKeys = accessedPks;
        this.keyDecoderProvider = getKeyDecoder(accessedPks, keyColumnEncodingOrder,
                keyColumnTypes, VersionedSerializers.typesForVersion(tableVersion));
        this.tableVersion = tableVersion;
        if(filterFactory==null){
            this.filterFactory = new SIFilterFactory<Data>() {
                @Override
                public SIFilter<Data> newFilter(EntryPredicateFilter predicateFilter,
                                                EntryDecoder rowEntryDecoder,
                                                EntryAccumulator accumulator,
                                                boolean isCountStar) throws IOException {
                    TxnFilter<Data> txnFilter = region.unpackedFilter(txn);

                    HRowAccumulator<Data> hRowAccumulator = new HRowAccumulator<Data>(dataLib,predicateFilter,
                                                                                      rowEntryDecoder, accumulator,
                                                                                      isCountStar);
                    //noinspection unchecked
                    return new PackedTxnFilter<Data>(txnFilter, hRowAccumulator){
                        @Override
                        public Filter.ReturnCode doAccumulate(Data dataKeyValue) throws IOException {
                            if (!accumulator.isFinished() && accumulator.isOfInterest(dataKeyValue)) {
                                if (!accumulator.accumulate(dataKeyValue)) {
                                    return Filter.ReturnCode.NEXT_ROW;
                                }
                                return Filter.ReturnCode.INCLUDE;
                            }else return Filter.ReturnCode.INCLUDE;
                        }
                    };
                }
            };
        }
        else
            this.filterFactory = filterFactory;
    }

    @Override
    public void open() throws StandardException, IOException {

    }

    @Override
    public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        SIFilter filter = getSIFilter();
        if(keyValues==null)
            keyValues = Lists.newArrayListWithExpectedSize(2);
        boolean hasRow;
        do{
            keyValues.clear();
            template.resetRowArray(); //necessary to deal with null entries--maybe make the underlying call faster?
            hasRow = dataLib.regionScannerNext(regionScanner, keyValues);
            if(keyValues.size()<=0){
                currentRowLocation = null;
                return null;
            }else{
                Data currentKeyValue = keyValues.get(0);
                updatePredicateFilterIfNecessary(currentKeyValue);
                if(template.nColumns()>0){
                    if(!filterRowKey(currentKeyValue)||!filterRow(filter)){
                        //filter the row first, then filter the row key
                        //filterCounter.increment();
                        continue;
                    }
                }else if(!filterRow(filter)){
                    //still need to filter rows to deal with transactional issues
                    //filterCounter.increment();
                    continue;
                } else {
                    if (LOG.isTraceEnabled())
                        SpliceLogUtils.trace(LOG,"miss columns=%d",template.nColumns());
                }
                currentKeyValue = keyValues.get(0);
                setRowLocation(currentKeyValue);
                return template;
            }
        }while(hasRow);
        currentRowLocation = null;
        return null;
    }

    public void recordFieldLengths(int[] columnLengths){
        if(rowDecodingMap!=null) {
            for (int i = 0; i < rowDecodingMap.length; i++) {
                int pos = rowDecodingMap[i];
                if(pos<0) continue;
                columnLengths[pos] = accumulator.getCurrentLength(i);
            }
        }
        if(keyDecodingMap!=null) {
            for (int i = 0; i < keyDecodingMap.length; i++) {
                int pos = keyDecodingMap[i];
                if(pos<0) continue;
                columnLengths[pos] = keyAccumulator.getCurrentLength(i);
            }
        }
    }

    public RowLocation getCurrentRowLocation(){
        return currentRowLocation;
    }


    //TODO -sf- add a nextBatch() method

    @Override
    public void close() throws StandardException, IOException {
        if(keyAccumulator!=null)
            keyAccumulator.close();
        if(siFilter!=null)
            siFilter.getAccumulator().close();
    }

    public TimeView getTime(){
        return regionScanner.getReadTime();
    }

    public long getRowsFiltered(){
        return 0;
    }

    public long getRowsVisited() {
        return regionScanner.getRowsVisited();
    }

    public void setRegionScanner(MeasuredRegionScanner scanner){
        this.regionScanner = scanner;
    }


    public long getBytesVisited() {
        return regionScanner.getBytesVisited();
    }

    public MeasuredRegionScanner<Data> getRegionScanner() {
        return regionScanner;
    }

    /*********************************************************************************************************************/
		/*Private helper methods*/
    private Supplier<MultiFieldDecoder> getKeyDecoder(FormatableBitSet accessedPks,
                                                      int[] allPkColumns,
                                                      int[] keyColumnTypes,
                                                      TypeProvider typeProvider) {
        if(accessedPks==null||accessedPks.getNumBitsSet()<=0){
            isKeyed = false;
            return null;
        }

        primaryKeyIndex = getIndex(allPkColumns,keyColumnTypes,typeProvider);

        keyDecoder = MultiFieldDecoder.create();
        return Suppliers.ofInstance(keyDecoder);
    }

    private KeyIndex getIndex(final int[] allPkColumns, int[] keyColumnTypes,TypeProvider typeProvider) {
        return new KeyIndex(allPkColumns,keyColumnTypes, typeProvider);
    }

    @SuppressWarnings("unchecked")
    private SIFilter getSIFilter() throws IOException {
        if(siFilter==null) {
            boolean isCountStar = scan.getAttribute(SIConstants.SI_COUNT_STAR)!=null;
            predicateFilter= buildInitialPredicateFilter();
            accumulator = ExecRowAccumulator.newAccumulator(predicateFilter, false, template, rowDecodingMap, tableVersion);
            siFilter = filterFactory.newFilter(predicateFilter,getRowEntryDecoder(),accumulator,isCountStar);
        }
        return siFilter;
    }

    /* SkippingScanFilter can have different predicates for every range */
    private void updatePredicateFilterIfNecessary(Data kv) throws IOException {
        if(scopedPredicates.isScanWithScopedPredicates()) {
            predicateFilter.setValuePredicates(scopedPredicates.getNextPredicates(kv));
        }
    }

    protected EntryDecoder getRowEntryDecoder() {
        return new EntryDecoder();
    }

    private EntryPredicateFilter buildInitialPredicateFilter() throws IOException {
        scopedPredicates = new ScopedPredicates(Scans.findSkippingScanFilter(this.scan),dataLib);

        EntryPredicateFilter entryPredicateFilter = EntryPredicateFilter.fromBytes(scan.getAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL));
        BitSet checkedColumns = entryPredicateFilter.getCheckedColumns();
        if(scopedPredicates.isScanWithScopedPredicates()){
                /*
                 * We have to be careful--EMPTY_PREDICATE could have been returned, in which case
                 * setting predicates can cause all kinds of calamituous behavior. To avoid that, when
                 * we have a skipscan filter (e.g. some kind of block scanning like a Batch NLJ) and we
                 * have an empty Predicate Filter to begin with, then we clone the predicate filter to a new
                 * one to avoid contamination.
                 *
                 */
            return new EntryPredicateFilter(checkedColumns,new ObjectArrayList<Predicate>(),entryPredicateFilter.indexReturned());
        }
        return entryPredicateFilter;
    }

    protected void setRowLocation(Data sampleKv) throws StandardException {
        if(indexName!=null && template.nColumns() > 0 && template.getColumn(template.nColumns()).getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID){
            /*
			 * If indexName !=null, then we are currently scanning an index,
			 * so our RowLocation should point to the main table, and not to the
			 * index (that we're actually scanning)
			 */
            if (template.getColumn(template.nColumns()).getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID) {
                currentRowLocation = (RowLocation) template.getColumn(template.nColumns());
            } else {
                try {
                    if (entryDecoder == null)
                        entryDecoder = new EntryDecoder();
                    entryDecoder.set(dataLib.getDataValueBuffer(sampleKv),
                            dataLib.getDataValueOffset(sampleKv),
                            dataLib.getDataValuelength(sampleKv));

                    MultiFieldDecoder decoder = entryDecoder.getEntryDecoder();
                    slice.set(decoder.decodeNextBytesUnsorted());
                    if(currentRowLocation==null)
                        currentRowLocation = new HBaseRowLocation(slice);
                    else
                        currentRowLocation.setValue(slice);
                }
                catch (IOException e) {
                    throw StandardException.newException(e.getMessage());
                }
            }
        } else {
            slice.set(dataLib.getDataRowBuffer(sampleKv), dataLib.getDataRowOffset(sampleKv),
                    dataLib.getDataRowlength(sampleKv));
            if(currentRowLocation==null)
                currentRowLocation = new HBaseRowLocation(slice);
            else
                currentRowLocation.setValue(slice);
        }
    }

    private boolean filterRow(SIFilter filter) throws IOException {
        filter.nextRow();
        Iterator<Data> kvIter = keyValues.iterator();
        while(kvIter.hasNext()){
            Data kv = kvIter.next();
            Filter.ReturnCode returnCode = filter.filterKeyValue(kv);
            switch(returnCode){
                case NEXT_COL:
                case NEXT_ROW:
                case SEEK_NEXT_USING_HINT:
                    return false; //failed the predicate
                case SKIP:
                    kvIter.remove();
                default:
                    //these are okay--they mean the encoding is good
            }
        }
        return keyValues.size() > 0 && filter.getAccumulator().result() != null;
    }

    private boolean filterRowKey(Data data) throws IOException {
        if(!isKeyed) return true;
        keyDecoder.set(dataLib.getDataRowBuffer(data), dataLib.getDataRowOffset(data), dataLib.getDataRowlength(data));
        if(keyAccumulator==null)
            keyAccumulator = ExecRowAccumulator.newAccumulator(predicateFilter,false,template,
                    keyDecodingMap, keyColumnSortOrder, accessedKeys, tableVersion);
        keyAccumulator.reset();
        primaryKeyIndex.reset();
        return predicateFilter.match(primaryKeyIndex, keyDecoderProvider, keyAccumulator);
    }

    private class KeyIndex implements Indexed{
        private final int[] allPkColumns;
        private final int[] keyColumnTypes;
        private final TypeProvider typeProvider;
        private int position = 0;

        private final boolean[] sF;
        private final boolean[] fF;
        private final boolean[] dF;

        private KeyIndex(int[] allPkColumns,
                         int[] keyColumnTypes,
                         TypeProvider typeProvider) {
            this.allPkColumns = allPkColumns;
            this.keyColumnTypes = keyColumnTypes;
            this.typeProvider = typeProvider;
            sF = new boolean[keyColumnTypes.length];
            fF = new boolean[keyColumnTypes.length];
            dF = new boolean[keyColumnTypes.length];

            for(int i=0;i<sF.length;i++){
                sF[i] = typeProvider.isScalar(keyColumnTypes[i]);
                fF[i] = typeProvider.isFloat(keyColumnTypes[i]);
                dF[i] = typeProvider.isDouble(keyColumnTypes[i]);
            }
        }

        @Override public int nextSetBit(int currentPosition) {
            if(position>=allPkColumns.length)
                return -1;
            int pos =position;
            position++;
            return pos;
        }

        @Override public boolean isScalarType(int currentPosition) {
            return sF[currentPosition];
        }

        @Override public boolean isDoubleType(int currentPosition) {
            return dF[currentPosition];
        }

        @Override public boolean isFloatType(int currentPosition) {
            return fF[currentPosition];
        }

        @Override
        public int getPredicatePosition(int position) {
            return allPkColumns[position];
        }

        void reset(){
            position=0;
        }
    }

}
