package com.splicemachine.derby.stream.stats;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.stats.collector.ColumnStatsCollector;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class IndexStatisticsCollector extends StatisticsCollector {

    long baseTableConglomId;
    private static final Logger LOG = Logger.getLogger(IndexStatisticsCollector.class);

    private final Random randomGenerator;
    private long numStarts = 0l;
    private final int sampleSize;
    private Timer fetchTimer;
    private HTableInterface baseTable;
    private final Get[] getBuffer;
    private int bufferPos = 0;

    public IndexStatisticsCollector(TxnView txn,
                                    ExecRow template,
                                    int[] columnPositionMap,
                                    int[] lengths,
                                    SITableScanner scanner,
                                    long baseConglomerateId) {
        super(txn, template, columnPositionMap, lengths, scanner);
        this.baseTableConglomId = baseTableConglomId;
        this.randomGenerator = new Random();
        int s = 1;
        while(s< SpliceConstants.indexBatchSize)s<<=1;
        this.sampleSize = s;
        this.fetchTimer = Metrics.newWallTimer();
        baseTable = SpliceAccessManager.getHTable(Long.toString(baseConglomerateId).getBytes());
        this.getBuffer = new Get[s];
    }

    @Override
    protected void updateRow(SITableScanner scanner,
                             ColumnStatsCollector<DataValueDescriptor>[] dvdCollectors,
                             int[] fieldLengths, ExecRow row) throws StandardException, IOException {
        super.updateRow(scanner, dvdCollectors, fieldLengths, row);
        sampledGet(row);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private double getLatency() throws IOException{
        if(bufferPos>0){
            doFetch(bufferPos);
            bufferPos=0;
        }
        TimeView time = fetchTimer.getTime();
        double latency;
        if(fetchTimer.getNumEvents()<=0)latency = 0d;
        else
            latency = ((double) time.getWallClockTime()) / fetchTimer.getNumEvents();
        if(LOG.isTraceEnabled())
            LOG.trace("IndexLookup latency = "+ time.getWallClockTime()+" nanoseconds to retrieve "+fetchTimer.getNumEvents()+" events");
        return latency;
    }

    @Override
    protected long getRemoteReadTime(long rowCount) throws ExecutionException {
        try{
            return (long)(getLatency()*rowCount);
        }catch(IOException e){
            throw new ExecutionException(e);
        }
    }

    @Override
    protected long getOpenScannerEvents(){
        return fetchTimer.getNumEvents();
    }

    @Override
    protected long getCloseScannerTimeMicros(){
        return 0l;
    }

    @Override
    protected long getOpenScannerTimeMicros() throws ExecutionException{
        return 0l;
    }

    @Override
    protected long getCloseScannerEvents(){
        return 0l;
    }

    protected void sampledGet(ExecRow row) throws StandardException, IOException{
        /*
         * There's a weird behavior with system tables where the row location could be empty.
         * This is very weird, and we don't like it much, but it happens and we don't want to blow
         * up just because. Thus, we skip those rows (we don't sample them either, so that we don't affect
         * the sample of the base table).
         */
        DataValueDescriptor dvd = row.getColumn(row.nColumns());
        if(!(dvd instanceof RowLocation)) return;
        RowLocation rl = (RowLocation)dvd;
        byte[] rowLocation = rl.getBytes();
        if(rowLocation==null || rowLocation.length<=0) return; //this is weird, but it COULD happen hypothetically

        if(!isSampled()) return;
        getBuffer[bufferPos] = TransactionOperations.getOperationFactory().newGet(txn,rowLocation);
        bufferPos =(bufferPos+1) & (sampleSize-1);
        if(bufferPos==0){
            doFetch(sampleSize);
        }
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
        long pos = (long)(randomGenerator.nextDouble()*numStarts);
        return pos < sampleSize;
    }

    private void doFetch(int bufferSize) throws IOException{
        if(bufferSize<=0) return; //nothing to do
        List<Get> gets= Arrays.asList(getBuffer).subList(0,bufferSize);
        /*
         * The JVM may not necessarily have heated up this code path just yet, and the HBase
         * memstore may not have the rows cached in memory. This means that we will tend to take longer
         * here when we first run than when the future occurs. To avoid this issue, we want to repeatedly
         * call this code path to make sure that everything heats up and we get a more accurate read of what's
         * going to happen in the long term. In the future, we will get this number by reference
         * to some kind of index lookup service which keeps a running tally of the average latency, but for now,
         * we do this repetition.
         *
         * Of course, this makes collections much more expensive, but accuracy is important, since it will
         * determine whether or not an index is selected in the optimizer
         *
         */
        fetchTimer.startTiming();
        Result[] results=baseTable.get(gets);
        fetchTimer.tick(results.length);
    }
}
