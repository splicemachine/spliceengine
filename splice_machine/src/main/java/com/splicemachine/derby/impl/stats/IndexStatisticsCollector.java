package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.Txn;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.collector.ColumnStatsCollector;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 3/10/15
 */
public class IndexStatisticsCollector extends StatisticsCollector {
    private static final Logger LOG = Logger.getLogger(IndexStatisticsCollector.class);

    private final Random randomGenerator;
    private long numStarts = 0l;
    private final int sampleSize;
    private Timer fetchTimer;
    private HTableInterface baseTable;

    public IndexStatisticsCollector(Txn txn,
                                    ExecRow colsToCollect,
                                    Scan partitionScan,
                                    int[] rowDecodingMap,
                                    int[] keyColumnEncodingOrder,
                                    boolean[] keyColumnSortOrder,
                                    int[] keyColumnTypes,
                                    int[] keyDecodingMap,
                                    int[] columnPositionMap,
                                    int[] fieldLengths,
                                    FormatableBitSet collectedKeyColumns,
                                    String tableVersion,
                                    TransactionalRegion txnRegion,
                                    MeasuredRegionScanner scanner,
                                    long baseConglomerateId) {
        super(txn,
                colsToCollect,
                partitionScan,
                rowDecodingMap,
                keyColumnEncodingOrder,
                keyColumnSortOrder,
                keyColumnTypes,
                keyDecodingMap,
                columnPositionMap,
                fieldLengths,
                collectedKeyColumns,
                tableVersion,
                txnRegion,
                scanner);
        this.randomGenerator = new Random();
        this.sampleSize = StatsConstants.fetchSampleSize;
        this.fetchTimer = Metrics.newWallTimer();
        baseTable = SpliceAccessManager.getHTable(Long.toString(baseConglomerateId).getBytes());
    }

    @Override
    protected void updateRow(SITableScanner scanner,
                             ColumnStatsCollector<DataValueDescriptor>[] dvdCollectors,
                             int[] fieldLengths,
                             ExecRow row) throws StandardException, IOException {
        super.updateRow(scanner,dvdCollectors,fieldLengths,row);
        if(!isSampled()) return;

        RowLocation rl = (RowLocation)row.getColumn(row.nColumns());
        byte[] rowLocation = rl.getBytes();
        Get get = TransactionOperations.getOperationFactory().newGet(txn,rowLocation);
        fetchTimer.startTiming();
        baseTable.get(get);
        fetchTimer.tick(1);
    }

    @Override
    protected void closeResources(){
        super.closeResources();
        try {
            baseTable.close();
        } catch (IOException e) {
            LOG.error("Encountered error closing base htable",e);
        }
    }

    @Override
    protected List<ColumnStatistics> getFinalColumnStats(ColumnStatsCollector<DataValueDescriptor>[] dvdCollectors){
        /*
         * The last column in the row is the HBaseRowLocation of the base table, which we do not include statistics
         * for. Hence, we ignore it (for now)
         *
         * TODO -sf- should we collect the avg column width to improve our costing model?
         */
        List<ColumnStatistics> columnStats = new ArrayList<>(dvdCollectors.length-1);
        for (int i = 0; i < dvdCollectors.length-1; i++) {
            columnStats.add(dvdCollectors[i].build());
        }
        return columnStats;
    }

    @Override
    protected long getOpenScannerTimeMicros(){
        return (long)getLatency();
    }

    @Override
    protected long getCloseScannerTimeMicros(){
        return (long)getLatency(); //TODO -sf- is this correct?
    }

    @Override
    protected long getRemoteReadTime(long rowCount) {
        double latency=getLatency();
        double est = latency*rowCount;
        return Math.round(est);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private double getLatency(){
        TimeView time = fetchTimer.getTime();
        double latency;
        if(fetchTimer.getNumEvents()<=0)latency = 0d;
        else
            latency = ((double) time.getWallClockTime()) / fetchTimer.getNumEvents();
        return latency;
    }

    private boolean isSampled() {
        /*
         * This uses Vitter's Resevoir sampling to generate a uniform random sample
         * of data points, which we can use to generate a small sample of data to fetch (and
         * thus giving us our remote read latency)
         */
        if(this.numStarts<sampleSize) {
            numStarts++;
            return true;
        }

        numStarts++;
        long pos = (long)randomGenerator.nextDouble()*numStarts;
        return pos < sampleSize;
    }
}
