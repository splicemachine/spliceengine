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
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.metrics.*;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
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
                             final String tableVersion) {
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
                             final long demarcationPoint) {
        this(scanner, region, template, scan, rowDecodingMap, txn, keyColumnEncodingOrder,
                keyColumnSortOrder, keyColumnTypes, keyDecodingMap, accessedPks, reuseRowLocation, indexName,
                tableVersion);
        this.demarcationPoint = demarcationPoint;
        /*
        if(filterFactory==null)
            this.filterFactory = createFilterFactory(txn, demarcationPoint);
            */
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
                             final long demarcationPoint,
                             DataValueDescriptor optionalProbeValue) {
        this(scanner, region, template, scan, rowDecodingMap, txn, keyColumnEncodingOrder,
                keyColumnSortOrder, keyColumnTypes, keyDecodingMap, accessedPks, reuseRowLocation, indexName,
                tableVersion,demarcationPoint);
        this.optionalProbeValue = optionalProbeValue;
    }

    @Override
    public void open() throws StandardException, IOException {

    }

    @Override
    public ExecRow next() throws StandardException, IOException {
            template.resetRowArray(); //necessary to deal with null entries--maybe make the underlying call faster?
            Record record =regionScanner.next();
        throw new UnsupportedOperationException("Not Implemented");
//        return record.getData();
    }

    public long getBytesOutput(){
        return outputBytesCounter.getTotal();
    }

    public RowLocation getCurrentRowLocation(){
        return currentRowLocation;
    }


    @Override
    public void close() throws StandardException, IOException {
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

    public void setRegionScanner(RecordScanner scanner){
        this.regionScanner = scanner;
    }

    public long getBytesVisited() {
        return regionScanner.getBytesOutput();
    }

    public RecordScanner getRegionScanner() {
        return regionScanner;
    }


}
