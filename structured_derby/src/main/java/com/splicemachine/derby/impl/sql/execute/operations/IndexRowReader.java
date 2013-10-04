package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * Utility for executing "look-ahead" index lookups, where the index lookup is backgrounded,
 * while other processes occur on the caller thread.
 *
 * @author Scott Fines
 * Created on: 9/4/13
 */
class IndexRowReader {
    private final ExecutorService lookupService;
    private final SpliceOperation sourceOperation;
    private final int batchSize;
    private final int numBlocks;
    private final ExecRow outputTemplate;
    private final String txnId;
    private final int[] indexCols;
    private final long mainTableConglomId;
    private final int[] adjustedBaseColumnMap;
    private final byte[] predicateFilterBytes;

    private List<Pair<RowAndLocation,Result>> currentResults;
    private RowAndLocation toReturn = new RowAndLocation();
    private List<Future<List<Pair<RowAndLocation,Result>>>> resultFutures;

    private HTableInterface table;
    private boolean populated = false;
    private MultiFieldDecoder fieldDecoder;

    private static final MetricName fetchTimerName = new MetricName("com.splicemachine.operations","indexLookup","fetchTime");
    private final Timer fetchTimer = SpliceDriver.driver().getRegistry().newTimer(fetchTimerName,TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
    private EntryDecoder entryDecoder;

    private IndexRowReader(ExecutorService lookupService,
                          SpliceOperation sourceOperation,
                          int batchSize,
                          int numBlocks,
                          ExecRow template,
                          String txnId,
                          int[] indexCols,
                          long mainTableConglomId,
                          int[] adjustedBaseColumnMap,
                          byte[] predicateFilterBytes) {
        this.lookupService = lookupService;
        this.sourceOperation = sourceOperation;
        this.batchSize = batchSize;
        this.numBlocks = numBlocks;
        this.outputTemplate = template;
        this.txnId = txnId;
        this.indexCols = indexCols;
        this.mainTableConglomId = mainTableConglomId;
        this.adjustedBaseColumnMap = adjustedBaseColumnMap;
        this.resultFutures = Lists.newArrayListWithExpectedSize(numBlocks);

        this.predicateFilterBytes = predicateFilterBytes;
    }

    public static IndexRowReader create(SpliceOperation sourceOperation,
                                        long mainTableConglomId,
                                        ExecRow template,
                                        String txnId,
                                        int[] indexCols,
                                        int[] adjustedBaseColumnMap,
                                        FormatableBitSet heapOnlyCols){
        int numBlocks = SpliceConstants.indexLookupBlocks;
        int batchSize = SpliceConstants.indexBatchSize;

        ThreadFactoryBuilder factoryBuilder = new ThreadFactoryBuilder();
        ThreadFactory factory = factoryBuilder.setNameFormat("index-lookup-%d").build();
        ExecutorService backgroundService = new ThreadPoolExecutor(numBlocks,numBlocks,60,TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),factory,new ThreadPoolExecutor.CallerRunsPolicy());

        BitSet fieldsToReturn = new BitSet(heapOnlyCols.getNumBitsSet());
        for(int i=heapOnlyCols.anySetBit();i>=0;i=heapOnlyCols.anySetBit(i)){
            fieldsToReturn.set(i);
        }
        EntryPredicateFilter epf = new EntryPredicateFilter(fieldsToReturn, Collections.<Predicate>emptyList());
        byte[] predicateFilterBytes = epf.toBytes();
        return new IndexRowReader(backgroundService,
                sourceOperation,
                batchSize,numBlocks,
                template,txnId,
                indexCols,mainTableConglomId,
                adjustedBaseColumnMap,predicateFilterBytes);
    }

    public void close() throws IOException {
        if(entryDecoder!=null)
            entryDecoder.close();
        if(table!=null)
            table.close();
        lookupService.shutdownNow();
    }

    public RowAndLocation next() throws StandardException, IOException {
        if(currentResults==null||currentResults.size()<=0)
            getMoreData();

        if(currentResults ==null ||currentResults.size()<=0)
            return null; //no more data to return

        Pair<RowAndLocation,Result> next = currentResults.remove(0);

        //merge the results
        RowAndLocation nextScannedRow = next.getFirst();
        Result nextFetchedData = next.getSecond();

        if(entryDecoder==null)
            entryDecoder = new EntryDecoder(SpliceDriver.getKryoPool());

        for(KeyValue kv:nextFetchedData.raw()){
            RowMarshaller.sparsePacked().decode(kv,nextScannedRow.row.getRowArray(),adjustedBaseColumnMap,entryDecoder);
        }

        return nextScannedRow;
    }

    private void getMoreData() throws StandardException, IOException {
        //read up to batchSize rows from the source, then submit them to the background thread for processing
        List<RowAndLocation> sourceRows = Lists.newArrayListWithCapacity(batchSize);
        for(int i=0;i<batchSize;i++){
            ExecRow next = sourceOperation.nextRow(null);
            if(next==null) break; //we are done

            if(!populated){
                for(int index=0;index<indexCols.length;index++){
                    if(indexCols[index]!=-1){
                        outputTemplate.setColumn(index + 1, next.getColumn(indexCols[index] + 1));
                    }
                }
                populated=true;
                toReturn.row = outputTemplate;
            }
            RowAndLocation rowLoc = new RowAndLocation();
            rowLoc.row = outputTemplate.getClone();
            rowLoc.rowLocation = next.getColumn(next.nColumns()).getBytes();
            sourceRows.add(rowLoc);
        }

        //submit to the background thread
        resultFutures.add(lookupService.submit(new Lookup(sourceRows)));

        //if there is only one submitted future, call this again to set off an additional background process
        if(resultFutures.size()<numBlocks && sourceRows.size()==batchSize)
            getMoreData();
        else{
            //wait for the first future to return correctly or error-out
            try {
                currentResults = resultFutures.remove(0).get();
            } catch (InterruptedException e) {
                throw Exceptions.parseException(e);
            } catch (ExecutionException e) {
                throw Exceptions.parseException(e);
            }
        }
    }

    static class RowAndLocation{
        ExecRow row;
        byte[] rowLocation;
    }

    private class Lookup implements Callable<List<Pair<RowAndLocation,Result>>> {
        private final List<RowAndLocation> sourceRows;

        public Lookup(List<RowAndLocation> sourceRows) {
            this.sourceRows = sourceRows;
            if(table==null)
                table = SpliceAccessManager.getHTable(mainTableConglomId);
        }

        @Override
        public List<Pair<RowAndLocation,Result>> call() throws Exception {
            long start = System.nanoTime();
            try{
                List<Get> gets = Lists.newArrayListWithCapacity(sourceRows.size());
                for(RowAndLocation sourceRow:sourceRows){
                    byte[] row = sourceRow.rowLocation;
                    Get get = SpliceUtils.createGet(txnId, row);
                    get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilterBytes);
                    gets.add(get);
                }

                Result[] results = table.get(gets);

                int i=0;
                List<Pair<RowAndLocation,Result>> locations = Lists.newArrayListWithCapacity(sourceRows.size());
                for(RowAndLocation sourceRow:sourceRows){
                    locations.add(Pair.newPair(sourceRow,results[i]));
                    i++;
                }
                return locations;
            }finally{
                fetchTimer.update(System.nanoTime()-start,TimeUnit.NANOSECONDS);
            }
        }
    }
}
