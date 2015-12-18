package com.splicemachine.derby.stream.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.stats.DvdStatsCollector;
import com.splicemachine.derby.impl.stats.SimpleOverheadManagedPartitionStatistics;
import com.splicemachine.derby.impl.stats.StatsConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.collector.ColumnStatsCollector;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;


public class StatisticsCollector {
    protected final TxnView txn;
    private final ExecRow template;
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
    private final SITableScanner scanner;
    private final String regionId;

    protected transient long openScannerTimeMicros = -1l;
    protected transient long closeScannerTimeMicros = -1l;
    private ColumnStatsCollector<DataValueDescriptor>[] dvdCollectors;
    private int[] fieldLengths;

    public StatisticsCollector(TxnView txn,
                               ExecRow template,
                               int[] columnPositionMap,
                               int[] lengths,
                               SITableScanner scanner) {
        this.txn = txn;
        this.template = template;
        this.columnPositionMap = columnPositionMap;
        this.lengths = lengths;
        this.scanner = scanner;
        MeasuredRegionScanner regionScanner = scanner.getRegionScanner();
        HRegionInfo region = regionScanner.getRegionInfo();
        String conglomId = region.getTable().getQualifierAsString();
        regionId = region.getEncodedName();
        tableConglomerateId = Long.parseLong(conglomId);
        dvdCollectors = getCollectors();
        fieldLengths = new int[dvdCollectors.length];
    }

    @SuppressWarnings("unchecked")
    public void collect(ExecRow row) throws ExecutionException {
        try{
            updateRow(scanner, dvdCollectors, fieldLengths, row);
        } catch (StandardException | IOException e) {
            throw new ExecutionException(e); //should only be IOExceptions
        }
    }

    public SimpleOverheadManagedPartitionStatistics getStatistics() throws ExecutionException {
        List<ColumnStatistics> columnStats = getFinalColumnStats(dvdCollectors);

        TimeView readTime = scanner.getTime();
        long byteCount = scanner.getBytesOutput();
        long rowCount = scanner.getRowsVisited() - scanner.getRowsFiltered();
        long localReadTimeMicros = readTime.getWallClockTime() / 1000; //scale to microseconds
        long remoteReadTimeMicros = getRemoteReadTime(rowCount);
        if (remoteReadTimeMicros > 0) {
            remoteReadTimeMicros /= 1000;
        }
        return new SimpleOverheadManagedPartitionStatistics(
                (new Long(tableConglomerateId)).toString(),
                regionId,
                rowCount,
                byteCount,
                columnStats);

    }
    protected long getCloseScannerEvents(){ return 1l; }

    protected long getOpenScannerTimeMicros() throws ExecutionException{ return openScannerTimeMicros; }
    protected long  getCloseScannerTimeMicros(){ return closeScannerTimeMicros; }

    protected long getOpenScannerEvents(){ return 1l; }

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
        int fetchSampleSize= StatsConstants.fetchSampleSize;
        Timer remoteReadTimer = Metrics.newWallTimer();
        Scan scan = TransactionOperations.getOperationFactory().newScan(txn);
        scan.setStartRow(HConstants.EMPTY_START_ROW);
        scan.setStopRow(HConstants.EMPTY_END_ROW);
        int n =2;
        long totalOpenTime = 0;
        long totalCloseTime = 0;
        int iterations = 0;
        try(Table table = SpliceAccessManager.getHTable(tableConglomerateId)){
            while(n<fetchSampleSize){
                scan.setBatch(n);
                scan.setCaching(n);
                long openTime=System.nanoTime();
                long closeTime;
                try(ResultScanner scanner=table.getScanner(scan)){
                    totalOpenTime+=(System.nanoTime()-openTime);
                    int pos=0;
                    remoteReadTimer.startTiming();
                    Result result;
                    while(pos<n && (result=scanner.next())!=null){
                        pos++;
                    }
                    remoteReadTimer.tick(pos);
                    closeTime=System.nanoTime();
                }
                totalCloseTime+=(System.nanoTime()-closeTime);
                iterations++;
                n<<=1;
            }
        }catch(IOException e){
            throw new ExecutionException(e);
        }
        //stash the scanner open and scanner close times
        openScannerTimeMicros=totalOpenTime/(1000*iterations);
        closeScannerTimeMicros=totalCloseTime/(1000*iterations);
        double latency=((double)remoteReadTimer.getTime().getWallClockTime())/remoteReadTimer.getNumEvents();
        return Math.round(latency*rowCount);
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
            collectors[i] = DvdStatsCollector.newCollector(columnId, dvd.getTypeFormatId(), columnLength, topKSize, cardinalityPrecision);
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
}
