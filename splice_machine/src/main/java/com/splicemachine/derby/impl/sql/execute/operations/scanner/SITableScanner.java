/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.si.api.txn.Txn;
import org.spark_project.guava.base.Suppliers;
import org.spark_project.guava.base.Throwables;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.metrics.*;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * TableScanner which applies SI to generate a row
 * @author Scott Fines
 * Date: 4/4/14
 */
public class SITableScanner<Data> implements StandardIterator<ExecRow>,AutoCloseable{
    public static ThreadLocal<String> regionId = new ThreadLocal<String>(){
        @Override
        protected String initialValue(){
            return "--";
        }
    };
    private static Logger LOG = Logger.getLogger(SITableScanner.class);
    private final Counter filterCounter;
    private RecordScanner regionScanner;
    private final TransactionalRegion region;
    private final RecordScan scan;
    protected final ExecRow template;
    private final boolean reuseRowLocation;
    private final String tableVersion;
    protected final int[] rowDecodingMap;
    protected RowLocation currentRowLocation;
    private final boolean[] keyColumnSortOrder;
    private String indexName;
    private ByteSlice slice = new ByteSlice();
    private boolean isKeyed = true;
    private int[] keyDecodingMap;
    private FormatableBitSet accessedKeys;
    private SIFilterFactory filterFactory;
    private ExecRowAccumulator accumulator;
    private final Counter outputBytesCounter;
    private long demarcationPoint;
    private DataValueDescriptor optionalProbeValue;

    protected SITableScanner(RecordScanner scanner,
                             final TransactionalRegion region,
                             final ExecRow template,
                             RecordScan scan,
                             final int[] rowDecodingMap,
                             final Txn txn,
                             int[] keyColumnEncodingOrder,
                             boolean[] keyColumnSortOrder,
                             int[] keyColumnTypes,
                             int[] keyDecodingMap,
                             FormatableBitSet accessedPks,
                             boolean reuseRowLocation,
                             String indexName,
                             final String tableVersion,
                             SIFilterFactory filterFactory) {
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
        this.tableVersion = tableVersion;
        if(filterFactory==null){
            this.filterFactory = createFilterFactory(txn, demarcationPoint);
        }
        else
            this.filterFactory = filterFactory;
    }

    protected SITableScanner(RecordScanner scanner,
                             final TransactionalRegion region,
                             final ExecRow template,
                             RecordScan scan,
                             final int[] rowDecodingMap,
                             final Txn txn,
                             int[] keyColumnEncodingOrder,
                             boolean[] keyColumnSortOrder,
                             int[] keyColumnTypes,
                             int[] keyDecodingMap,
                             FormatableBitSet accessedPks,
                             boolean reuseRowLocation,
                             String indexName,
                             final String tableVersion,
                             SIFilterFactory filterFactory,
                             final long demarcationPoint) {
        this(scanner, region, template, scan, rowDecodingMap, txn, keyColumnEncodingOrder,
                keyColumnSortOrder, keyColumnTypes, keyDecodingMap, accessedPks, reuseRowLocation, indexName,
                tableVersion, filterFactory);
        this.demarcationPoint = demarcationPoint;
        if(filterFactory==null)
            this.filterFactory = createFilterFactory(txn, demarcationPoint);
    }

    protected SITableScanner(RecordScanner scanner,
                             final TransactionalRegion region,
                             final ExecRow template,
                             RecordScan scan,
                             final int[] rowDecodingMap,
                             final Txn txn,
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
                             DataValueDescriptor optionalProbeValue) {
        this(scanner, region, template, scan, rowDecodingMap, txn, keyColumnEncodingOrder,
                keyColumnSortOrder, keyColumnTypes, keyDecodingMap, accessedPks, reuseRowLocation, indexName,
                tableVersion, filterFactory,demarcationPoint);
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
                measureOutputSize(keyValues);
                currentKeyValue = keyValues.get(0);
                setRowLocation(currentKeyValue);
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


    @Override
    public void close() throws StandardException, IOException {
        if(keyAccumulator!=null)
            keyAccumulator.close();
        if(siFilter!=null)
            siFilter.getAccumulator().close();
        if (regionScanner != null)
            regionScanner.close();
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
    private SIFilterFactory createFilterFactory(Txn txn, long demarcationPoint) {
        Txn txnView = txn;
        if (demarcationPoint > 0) {
            txnView = new DDLTxnView(txn,demarcationPoint);
        }

        SIFilterFactory siFilterFactory;
        try {
            final TxnFilter txnFilter = region.unpackedFilter(txnView);

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
                        @Override
                        public ReturnCode accumulate(DataCell data) throws IOException{
                            if (!accumulator.isFinished() && accumulator.isInteresting(data)) {
                                if (!accumulator.accumulateCell(data)) {
                                    return DataFilter.ReturnCode.NEXT_ROW;
                                }
                                return DataFilter.ReturnCode.INCLUDE;
                            } else return DataFilter.ReturnCode.INCLUDE;
                        }
                    };
                }
            };
        } catch (Exception e) {
            throw new RuntimeException(Throwables.getRootCause(e));
        }

        return siFilterFactory;
    }

    private KeyIndex getIndex(final int[] allPkColumns, int[] keyColumnTypes,TypeProvider typeProvider) {
        return new KeyIndex(allPkColumns,keyColumnTypes, typeProvider);
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

}
