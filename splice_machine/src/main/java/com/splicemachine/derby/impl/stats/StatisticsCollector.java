package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.SimplePartitionStatistics;
import com.splicemachine.stats.collector.ColumnStatsCollector;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Statistics collector entity. Designed to be shared by the two types of tasks (maintenance
 * and triggered collections).
 *
 * @author Scott Fines
 *         Date: 2/26/15
 */
public class StatisticsCollector {
    protected final TxnView txn;
    private final ExecRow template;
    private final Scan partitionScan;
    private final int[] rowDecodingMap;
    private final int[] keyColumnEncodingOrder;
    private final boolean[] keyColumnSortOrder;
    private final int[] keyColumnTypes;
    private final int[] keyDecodingMap;
    private final FormatableBitSet collectedKeyColumns;
    private final String tableVersion;
    private final TransactionalRegion txnRegion;
    private final MeasuredRegionScanner scanner;
    /*
     * A reverse mapping between the output column position and that column's position in
     * the original row.
     */
    private final int[] columnPositionMap;
    /*
     * The maximum length of any individual fields. -1 if there is no maximum length.
     *
     * This is primarily useful for string fields. Most other types will have no maximum length
     */
    private final int[] lengths;

    public StatisticsCollector(TxnView txn,
                               ExecRow template,
                               Scan partitionScan,
                               int[] rowDecodingMap,
                               int[] keyColumnEncodingOrder,
                               boolean[] keyColumnSortOrder,
                               int[] keyColumnTypes,
                               int[] keyDecodingMap,
                               int[] columnPositionMap,
                               int[] lengths,
                               FormatableBitSet collectedKeyColumns,
                               String tableVersion,
                               TransactionalRegion txnRegion,
                               MeasuredRegionScanner scanner
                               ) {
        this.txn = txn;
        this.template = template;
        this.partitionScan = partitionScan;
        this.rowDecodingMap = rowDecodingMap;
        this.keyColumnEncodingOrder = keyColumnEncodingOrder;
        this.keyColumnSortOrder = keyColumnSortOrder;
        this.keyColumnTypes = keyColumnTypes;
        this.keyDecodingMap = keyDecodingMap;
        this.collectedKeyColumns = collectedKeyColumns;
        this.tableVersion = tableVersion;
        this.txnRegion = txnRegion;
        this.scanner = scanner;
        this.columnPositionMap = columnPositionMap;
        this.lengths = lengths;
    }

    @SuppressWarnings("unchecked")
    public PartitionStatistics collect() throws ExecutionException {
        try(SITableScanner scanner = getTableScanner()){
            ColumnStatsCollector<DataValueDescriptor>[] dvdCollectors = getCollectors();
            int[] fieldLengths = new int[dvdCollectors.length];
            ExecRow row;
            while((row = scanner.next(null))!=null){
                updateRow(scanner, dvdCollectors, fieldLengths, row);
            }
            List<ColumnStatistics> columnStats = getFinalColumnStats(dvdCollectors);

            TimeView readTime = scanner.getTime();
            long byteCount = scanner.getBytesVisited();
            long rowCount = scanner.getRowsVisited();

            String tableId = txnRegion.getTableName();
            String regionId = txnRegion.getRegionName();
            long localReadTimeMicros = readTime.getWallClockTime() / 1000; //scale to microseconds
            long remoteReadTimeMicros = getRemoteReadTime(rowCount);
            if(remoteReadTimeMicros<0)
                remoteReadTimeMicros = localReadTimeMicros;
            else
                remoteReadTimeMicros/=1000;
            return new SimplePartitionStatistics(tableId,regionId,
                    rowCount,
                    byteCount,
                    0l, //TODO -sf- get Query count for this region
                    localReadTimeMicros,
                    remoteReadTimeMicros,
                    columnStats);
        } catch (StandardException | IOException e) {
            throw new ExecutionException(e); //should only be IOExceptions
        }finally{
            closeResources();
        }
    }

    protected void closeResources() {

    }

    protected void updateRow(SITableScanner scanner,
                             ColumnStatsCollector<DataValueDescriptor>[] dvdCollectors,
                             int[] fieldLengths,
                             ExecRow row) throws StandardException, IOException {
        scanner.recordFieldLengths(fieldLengths); //get the size of each column
        DataValueDescriptor[] dvds = row.getRowArray();
        for (int i = 0; i < dvds.length; i++) {
            DataValueDescriptor dvd = dvds[i];
            dvdCollectors[i].update(dvd);
            dvdCollectors[i].updateSize(fieldLengths[i]);
        }
    }

    protected long getRemoteReadTime(long rowCount) {
        return -1;
    }

    protected List<ColumnStatistics> getFinalColumnStats(ColumnStatsCollector<DataValueDescriptor>[] dvdCollectors) {
        List<ColumnStatistics> columnStats = new ArrayList<>(dvdCollectors.length);
        for (int i = 0; i < dvdCollectors.length; i++) {
            columnStats.add(dvdCollectors[i].build());
        }
        return columnStats;
    }

    protected void populateCollectors(DataValueDescriptor[] dvds, ColumnStatsCollector<DataValueDescriptor>[] collectors) {
        int cardinalityPrecision = StatsConstants.cardinalityPrecision;
        int topKSize = StatsConstants.topKSize;
        for(int i=0;i<dvds.length;i++){
            DataValueDescriptor dvd = dvds[i];
            int columnId = columnPositionMap[i];
            int columnLength = lengths[i];
            collectors[i] = DvdStatsCollector.newCollector(columnId, dvd.getTypeFormatId(), columnLength, cardinalityPrecision, topKSize);
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    @SuppressWarnings("unchecked")
    private ColumnStatsCollector<DataValueDescriptor>[] getCollectors() {
        DataValueDescriptor[] dvds = template.getRowArray();
        ColumnStatsCollector<DataValueDescriptor> [] collectors = new ColumnStatsCollector[dvds.length];
        populateCollectors(dvds, collectors);
        return collectors;
    }

    @SuppressWarnings("unchecked")
    private SITableScanner getTableScanner() {
        return new TableScannerBuilder()
                .transaction(txn)
                .template(template)
                .metricFactory(Metrics.basicMetricFactory()) //record latency timings
                .scan(partitionScan)
                .rowDecodingMap(rowDecodingMap)
                .keyColumnEncodingOrder(keyColumnEncodingOrder)
                .keyColumnSortOrder(keyColumnSortOrder)
                .keyColumnTypes(keyColumnTypes)
                .keyDecodingMap(keyDecodingMap)
                .accessedKeyColumns(collectedKeyColumns)
                .tableVersion(tableVersion)
                .region(txnRegion)
                .scanner(scanner)
                .build();
    }
}
