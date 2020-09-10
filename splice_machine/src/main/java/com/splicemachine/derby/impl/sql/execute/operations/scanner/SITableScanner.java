/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import splice.com.google.common.base.Supplier;
import splice.com.google.common.base.Suppliers;
import splice.com.google.common.base.Throwables;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.store.ExecRowAccumulator;
import com.splicemachine.db.iapi.types.HBaseRowLocation;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.metrics.*;
import com.splicemachine.si.api.filter.SIFilter;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.filter.HRowAccumulator;
import com.splicemachine.si.impl.txn.DDLTxnView;
import com.splicemachine.si.impl.filter.PackedTxnFilter;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * TableScanner which applies SI to generate a row
 * @author Scott Fines
 * Date: 4/4/14
 */
public class SITableScanner<Data> implements StandardIterator<ExecRow>,AutoCloseable{
    public static final ThreadLocal<String> regionId = new ThreadLocal<String>(){
        @Override
        protected String initialValue(){
            return "--";
        }
    };
    private static Logger LOG = Logger.getLogger(SITableScanner.class);
    private final Counter filterCounter;
    private DataScanner regionScanner;
    private final TransactionalRegion region;
    private final DataScan scan;
    protected final ExecRow template;
    private final boolean reuseRowLocation;
    private final String tableVersion;
    protected final int[] rowDecodingMap;
    private SIFilter siFilter;
    private EntryPredicateFilter predicateFilter;
    protected RowLocation currentRowLocation;
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
    private SIFilterFactory filterFactory;
    private ExecRowAccumulator accumulator;
    private EntryDecoder entryDecoder;
    private final Counter outputBytesCounter;
    private long demarcationPoint;
    private DataValueDescriptor optionalProbeValue;
    private ExecRow defaultRow;
    private FormatableBitSet defaultValueMap;

    protected SITableScanner(DataScanner scanner,
                             final TransactionalRegion region,
                             final ExecRow template,
                             DataScan scan,
                             final int[] rowDecodingMap,
                             final TxnView txn,
                             int[] keyColumnEncodingOrder,
                             boolean[] keyColumnSortOrder,
                             int[] keyColumnTypes,
                             int[] keyDecodingMap,
                             FormatableBitSet accessedPks,
                             boolean reuseRowLocation,
                             String indexName,
                             final String tableVersion,
                             SIFilterFactory filterFactory,
                             ExecRow defaultRow,
                             FormatableBitSet defaultValueMap,
                             boolean ignoreRecentTransactions
    ) {
        assert template!=null:"Template cannot be null into a scanner";
        this.region = region;
        regionId.set(region.getRegionName());
        this.scan = scan;
        this.template = template;
        this.rowDecodingMap = rowDecodingMap;
        this.keyColumnSortOrder = keyColumnSortOrder;
        this.indexName = indexName;
        this.reuseRowLocation = reuseRowLocation;
        MetricFactory metricFactory = Metrics.noOpMetricFactory();
        this.filterCounter = metricFactory.newCounter();
        this.outputBytesCounter = metricFactory.newCounter();
        this.regionScanner = scanner;
        this.keyDecodingMap = keyDecodingMap;
        this.accessedKeys = accessedPks;
        this.keyDecoderProvider = getKeyDecoder(accessedPks, keyColumnEncodingOrder,
                keyColumnTypes, VersionedSerializers.typesForVersion(tableVersion));
        this.tableVersion = tableVersion;
        if(filterFactory==null){
            this.filterFactory = createFilterFactory(txn, demarcationPoint, ignoreRecentTransactions);
        }
        else
            this.filterFactory = filterFactory;
        this.defaultRow = defaultRow;
        this.defaultValueMap = defaultValueMap;
    }

    protected SITableScanner(DataScanner scanner,
                             final TransactionalRegion region,
                             final ExecRow template,
                             DataScan scan,
                             final int[] rowDecodingMap,
                             final TxnView txn,
                             int[] keyColumnEncodingOrder,
                             boolean[] keyColumnSortOrder,
                             int[] keyColumnTypes,
                             int[] keyDecodingMap,
                             FormatableBitSet accessedPks,
                             boolean reuseRowLocation,
                             String indexName,
                             final String tableVersion,
                             SIFilterFactory filterFactory,
                             final long demarcationPoint,
                             ExecRow defaultRow,
                             FormatableBitSet defaultValueMap,
                             boolean ignoreRecentTransactions) {
        this(scanner, region, template, scan, rowDecodingMap, txn, keyColumnEncodingOrder,
                keyColumnSortOrder, keyColumnTypes, keyDecodingMap, accessedPks, reuseRowLocation, indexName,
                tableVersion, filterFactory, defaultRow, defaultValueMap, ignoreRecentTransactions);
        this.demarcationPoint = demarcationPoint;
        if(filterFactory==null)
            this.filterFactory = createFilterFactory(txn, demarcationPoint,ignoreRecentTransactions);
    }

    protected SITableScanner(DataScanner scanner,
                             final TransactionalRegion region,
                             final ExecRow template,
                             DataScan scan,
                             final int[] rowDecodingMap,
                             final TxnView txn,
                             int[] keyColumnEncodingOrder,
                             boolean[] keyColumnSortOrder,
                             int[] keyColumnTypes,
                             int[] keyDecodingMap,
                             FormatableBitSet accessedPks,
                             boolean reuseRowLocation,
                             String indexName,
                             final String tableVersion,
                             SIFilterFactory filterFactory,
                             final long demarcationPoint,
                             DataValueDescriptor optionalProbeValue,
                             ExecRow defaultRow,
                             FormatableBitSet defaultValueMap,
                             boolean ignoreRecentTransactions) {
        this(scanner, region, template, scan, rowDecodingMap, txn, keyColumnEncodingOrder,
                keyColumnSortOrder, keyColumnTypes, keyDecodingMap, accessedPks, reuseRowLocation, indexName,
                tableVersion, filterFactory,demarcationPoint, defaultRow, defaultValueMap, ignoreRecentTransactions);
        this.optionalProbeValue = optionalProbeValue;
    }

    @Override
    public void open() throws StandardException, IOException {

    }

    @Override
    public ExecRow next() throws StandardException, IOException {
        SIFilter filter = getSIFilter();
        do{
            template.resetRowArray(); //necessary to deal with null entries--maybe make the underlying call faster?
            List<DataCell> keyValues=regionScanner.next(-1);
            if(keyValues.size()<=0){
                currentRowLocation = null;
                return null;
            }else{
                DataCell currentKeyValue = keyValues.get(0);
                if(template.nColumns()>0){
                    if(!filterRowKey(currentKeyValue)||!filterRow(filter,keyValues)){
                        //filter the row first, then filter the row key
                        filterCounter.increment();
                        continue;
                    }
                }else if(!filterRow(filter,keyValues)){
                    //still need to filter rows to deal with transactional issues
                    filterCounter.increment();
                    continue;
                } else {
                    if (LOG.isTraceEnabled())
                        SpliceLogUtils.trace(LOG,"miss columns=%d",template.nColumns());
                }
                //fill the unpopulated non-null columns with default values
                if (defaultRow != null && defaultValueMap != null) {
                    for (int i=defaultValueMap.anySetBit(); i>=0; i=defaultValueMap.anySetBit(i)) {
                        if (template.getColumn(i+1).isNull())
                            template.setColumn(i+1, defaultRow.getColumn(i+1).cloneValue(false));
                    }
                }
                measureOutputSize(keyValues);
                currentKeyValue = keyValues.get(0);
                setRowLocation(currentKeyValue);
                template.setKey(currentRowLocation.getBytes());
                return template;
            }
        }while(true); //TODO -sf- this doesn't seem quite right
    }

    public long getBytesOutput(){
        return outputBytesCounter.getTotal();
    }

    private void measureOutputSize(List<DataCell> keyValues){
        if(outputBytesCounter.isActive()){
            for(DataCell cell:keyValues){
                outputBytesCounter.add(cell.encodedLength());
            }
        }

    }

    public RowLocation getCurrentRowLocation(){
        return currentRowLocation;
    }


    @Override
    public void close() throws StandardException, IOException {
        if(keyAccumulator!=null)
            keyAccumulator.close();
        if(siFilter!=null)
            siFilter.getAccumulator().close();
        if (regionScanner != null) {
            try {
                regionScanner.close();
            } catch (Exception e) {
                // ignore this exception, we are closing anyway
            }
        }
    }

    public TimeView getTime(){
        return regionScanner.getReadTime();
    }

    public long getRowsFiltered(){
        return filterCounter.getTotal();
    }

    public long getRowsVisited() {
        return regionScanner.getRowsVisited();
    }

    public void setRegionScanner(DataScanner scanner){
        this.regionScanner = scanner;
    }

    public long getBytesVisited() {
        return regionScanner.getBytesOutput();
    }

    public DataScanner getRegionScanner() {
        return regionScanner;
    }

    /*********************************************************************************************************************/
		/*Private helper methods*/
    private SIFilterFactory createFilterFactory(TxnView txn, long demarcationPoint,
                                                boolean ignoreRecentTransactions) {
        TxnView txnView = txn;
        if (demarcationPoint > 0) {
            txnView = new DDLTxnView(txn,demarcationPoint);
        }

        SIFilterFactory siFilterFactory;
        try {
            final TxnFilter txnFilter = region.unpackedFilter(txnView, ignoreRecentTransactions);

            siFilterFactory = new SIFilterFactory<Data>() {
                @Override
                public SIFilter newFilter(EntryPredicateFilter predicateFilter,
                                                EntryDecoder rowEntryDecoder,
                                                EntryAccumulator accumulator,
                                                boolean isCountStar) throws IOException {

                    HRowAccumulator hRowAccumulator =new HRowAccumulator(predicateFilter,
                            rowEntryDecoder,accumulator,
                            isCountStar);
                    //noinspection unchecked
                    return new PackedTxnFilter(txnFilter, hRowAccumulator) {
                        private boolean keyIncluded = false;

                        @Override
                        public ReturnCode filterCell(DataCell keyValue) throws IOException {
                            /**
                             * This logic tries to avoid performing transactional resolution when we are not actually interested
                             * in the value anyway. In some cases we read a column which the accumulator is not interested in
                             * but we have to transactionally resolve anyway, for instance if the field we are actually interested
                             * in is on the rowkey.
                             */
                            if (keyValue.dataType() == CellType.USER_DATA  && (!accumulator.isInteresting(keyValue) || accumulator.isFinished()) && keyIncluded){
                                return ReturnCode.INCLUDE;
                            }
                            return super.filterCell(keyValue);
                        }

                        @Override
                        public void nextRow() {
                            super.nextRow();
                            keyIncluded = false;
                        }

                        @Override
                        public ReturnCode accumulate(DataCell data) throws IOException{
                            if (!accumulator.isFinished() && accumulator.isInteresting(data)) {
                                if (!accumulator.accumulateCell(data)) {
                                    return DataFilter.ReturnCode.NEXT_ROW;
                                }
                            }
                            keyIncluded = true;
                            return DataFilter.ReturnCode.INCLUDE;
                        }
                    };
                }
            };
        } catch (Exception e) {
            throw new RuntimeException(Throwables.getRootCause(e));
        }

        return siFilterFactory;
    }
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

    protected EntryDecoder getRowEntryDecoder() {
        return new EntryDecoder();
    }

    private EntryPredicateFilter buildInitialPredicateFilter() throws IOException {
        return EntryPredicateFilter.fromBytes(scan.getAttribute(SIConstants.ENTRY_PREDICATE_LABEL));
    }

    protected void setRowLocation(DataCell sampleKv) throws StandardException {
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
                    entryDecoder.set(sampleKv.valueArray(),sampleKv.valueOffset(),sampleKv.valueLength());

                    MultiFieldDecoder decoder = entryDecoder.getEntryDecoder();
                    byte[] bytes = decoder.decodeNextBytesUnsorted();
                    if (reuseRowLocation) {
                        slice.set(bytes);
                    } else {
                        slice = ByteSlice.wrap(bytes);
                    }
                    if(currentRowLocation==null || !reuseRowLocation)
                        currentRowLocation = new HBaseRowLocation(slice);
                    else
                        currentRowLocation.setValue(slice);
                }
                catch (IOException e) {
                    throw StandardException.newException(e.getMessage());
                }
            }
        } else {
            if (reuseRowLocation) {
                slice.set(sampleKv.keyArray(),sampleKv.keyOffset(),sampleKv.keyLength());
            } else {
                slice = ByteSlice.wrap(sampleKv.keyArray(),sampleKv.keyOffset(),sampleKv.keyLength());
            }
            if(currentRowLocation==null || !reuseRowLocation)
                currentRowLocation = new HBaseRowLocation(slice);
            else
                currentRowLocation.setValue(slice);
        }
    }

    @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT",justification = "Intentional")
    private boolean filterRow(SIFilter filter,List<DataCell> keyValues) throws IOException {
        filter.nextRow();
        Iterator<DataCell> kvIter = keyValues.iterator();
        int numCells = keyValues.size();
        while(kvIter.hasNext()){
            DataCell kv = kvIter.next();
            DataFilter.ReturnCode returnCode = filter.filterCell(kv);
            switch(returnCode){
                case NEXT_COL:
                case NEXT_ROW:
                case SEEK:
                    return false; //failed the predicate
                case SKIP:
                    numCells--;
                default:
                    //these are okay--they mean the encoding is good
            }
        }
        return numCells > 0 && filter.getAccumulator().result() != null;
    }

    private boolean filterRowKey(DataCell data) throws IOException {
        if(!isKeyed) return true;
        keyDecoder.set(data.keyArray(), data.keyOffset(), data.keyLength());
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

        void reset(){
            position=0;
        }
    }

}
