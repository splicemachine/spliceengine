package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.SimplePartitionStatistics;
import com.splicemachine.stats.collector.ColumnStatsCollector;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
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
    private final long tableConglomerateId;

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
        this.tableConglomerateId=Long.parseLong(txnRegion.getTableName());
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

    protected long getRemoteReadTime(long rowCount) throws ExecutionException{
        /*
         * for base table scans, we want to do something akin to a self-join here. Really what we
         * need to do is read a sample of rows remotely, and measure their overall latency. That way,
         * we will know (roughly) the cost of reading this table.
         *
         * However, we don't want to measure the latency of reading from this region, since it would
         * be much more efficient than we normally would want. Instead, we try and pick regions
         * which do not contain the partitionScan. Of course, if the region contains everything,
         * then who cares.
         *
         * Once a region is selected, we read in some rows using a scanner, measure the average latency,
         * and then use that to estimate how long it would take to read us remotely
         */
        //TODO -sf- randomize this a bit so we don't hotspot a region
        try(HTableInterface table =SpliceAccessManager.getHTable(tableConglomerateId)){
            Scan scan =TransactionOperations.getOperationFactory().newScan(txn);
            scan.setStartRow(HConstants.EMPTY_START_ROW);
            scan.setStopRow(HConstants.EMPTY_END_ROW);
            int fetchSampleSize=StatsConstants.fetchSampleSize;
            scan.setCaching(fetchSampleSize);
            scan.setBatch(fetchSampleSize);
            Timer remoteReadTimer = Metrics.newWallTimer();
            try(ResultScanner scanner = table.getScanner(scan)){
                int pos = 0;
                remoteReadTimer.startTiming();
                Result result;
                while(pos<fetchSampleSize && (result = scanner.next())!=null){
                    pos++;
                }
                remoteReadTimer.tick(pos);
            }

            double latency = ((double)remoteReadTimer.getTime().getWallClockTime())/remoteReadTimer.getNumEvents();
            return Math.round(latency*rowCount);
        }catch(IOException e){
            throw new ExecutionException(e);
        }
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
