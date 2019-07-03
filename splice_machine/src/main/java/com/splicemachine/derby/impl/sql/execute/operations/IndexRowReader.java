/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.stream.function.IteratorUtils;
import com.splicemachine.si.impl.driver.SIDriver;
import org.spark_project.guava.collect.Lists;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.HBaseRowLocation;
import com.splicemachine.derby.utils.marshall.KeyDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import com.splicemachine.storage.util.MapAttributes;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Utility for executing "look-ahead" index lookups, where the index lookup is backgrounded,
 * while other processes occur on the caller thread.
 *
 * @author Scott Fines
 *         Created on: 9/4/13
 */
public class IndexRowReader implements Iterator<ExecRow>, Iterable<ExecRow>{
    protected static Logger LOG=Logger.getLogger(IndexRowReader.class);
    private final int batchSize;
    private final ExecRow outputTemplate;
    private final long mainTableConglomId;
    private final byte[] predicateFilterBytes;
    private final KeyDecoder [] keyDecoders;
    private final int[] indexCols;
    private final KeyHashDecoder [] rowDecoders;
    private final TxnView txn;
    private final TxnOperationFactory operationFactory;
    private final PartitionFactory tableFactory;
    protected volatile boolean doneReading = false;

    LookupResponse currentResults;
    private LinkedBlockingQueue<LookupResponse> currentResultsQueue =
                                                new LinkedBlockingQueue<>();
    private List<Future<LookupResponse>> resultFutures;
    private boolean populated=false;
    private EntryDecoder entryDecoder;
    protected Iterator<ExecRow> sourceIterator;
    private final ExecutorService executorService;
    private final CompletionService executorCompletionService;
    private final boolean useOldIndexLookupMethod;
    private int numConcurrentLookups = 0;
    private ConcurrentLinkedDeque<Integer> stackOfThreadNumbers =
                            new ConcurrentLinkedDeque<Integer>();

    private ExecRow heapRowToReturn;
    private ExecRow indexRowToReturn;
    private Future<Boolean> readerFuture;
    private boolean readerThreadStarted = false;

    IndexRowReader(Iterator<ExecRow> sourceIterator,
                   ExecRow outputTemplate,
                   TxnView txn,
                   int lookupBatchSize,
                   int numConcurrentLookups,
                   long mainTableConglomId,
                   byte[] predicateFilterBytes,
                   KeyHashDecoder[] keyDecoders,
                   KeyHashDecoder[] rowDecoders,
                   int[] indexCols,
                   TxnOperationFactory operationFactory,
                   PartitionFactory tableFactory,
                   boolean useOldIndexLookupMethod){
        this.sourceIterator=sourceIterator;
        this.outputTemplate=outputTemplate;
        this.txn=txn;
        batchSize=lookupBatchSize;
        this.numConcurrentLookups=Math.max(numConcurrentLookups, 2);
        this.mainTableConglomId=mainTableConglomId;
        this.predicateFilterBytes=predicateFilterBytes;
        this.tableFactory=tableFactory;
        this.keyDecoders = new KeyDecoder[this.numConcurrentLookups];
        for (int i=0; i < this.numConcurrentLookups; i++) {
            stackOfThreadNumbers.push(i);
            this.keyDecoders[i] = new KeyDecoder(keyDecoders[i], 0);
        }
        // Start with a dummy LookupResponse so we can avoid the overhead of
        // null pointer checks.
        currentResults = new LookupResponse(new ArrayList<>(), -1);
        this.rowDecoders =rowDecoders;
        this.indexCols=indexCols;
        this.resultFutures=Lists.newArrayListWithCapacity(this.numConcurrentLookups);
        //numConcurrentLookups = 32;  // msirek-temp
        this.numConcurrentLookups = numConcurrentLookups;
        this.resultFutures=Lists.newArrayListWithCapacity(numConcurrentLookups+1);
        this.operationFactory = operationFactory;
        this.useOldIndexLookupMethod = useOldIndexLookupMethod;

        executorService = Executors.newFixedThreadPool(numConcurrentLookups);
        executorCompletionService = new ExecutorCompletionService<>(executorService);
    }

    // Return the maximum number of threads that could be simultaneously
    // doing base conglomerate row lookups.
    public int getMaxConcurrency() {return this.numConcurrentLookups;}

    public void close() throws IOException{
        for (KeyHashDecoder rowDecoder:rowDecoders)
            rowDecoder.close();
        for (KeyDecoder keyDecoder:keyDecoders)
            keyDecoder.close();
        if(entryDecoder!=null)
            entryDecoder.close();

        if (!readerFuture.isDone())
            readerFuture.cancel(true);
    }

    protected void addResultsToQueue (LookupResponse response) {
        currentResultsQueue.add(response);
    }

    protected void getCurrentResultsFromQueue () throws StandardException {
        try {
            if (currentResults.isEmpty()) {
                LookupResponse response = null;
                if (!doneReading || !currentResultsQueue.isEmpty()) {
                //while (!doneReading) {
                //(response == null && (!doneReading || !currentResultsQueue.isEmpty())) {
                    // currentResults = currentResultsQueue.poll(50L, TimeUnit.MILLISECONDS); msirek-temp
                    //response = currentResultsQueue.poll();
                    //Thread.sleep(50L);
                    currentResults = currentResultsQueue.take();
                }
//                if (response != null)
//                    currentResults = response;
            }
        }
        catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public ExecRow next(){
        return heapRowToReturn;
    }

    public ExecRow nextScannedRow(){
        return indexRowToReturn;
    }

    @Override
    public void remove(){

    }

    @Override
    public boolean hasNext(){
        try{

            ExecRow row = currentResults.getNextRow();
            if (row != null) {
                heapRowToReturn  = row;
                indexRowToReturn = row;
                return true;
            }
            if (!readerThreadStarted) {
                AsynchIndexRowReader task = new AsynchIndexRowReader();
                readerFuture = SIDriver.driver().getExecutorService().submit(task);
                readerThreadStarted = true;
            }
            getCurrentResultsFromQueue();
            row = currentResults.getNextRow();

            if (row != null) {
                heapRowToReturn = row;
                indexRowToReturn = row;
                return true;
            }

            // No More Data
            return false;
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }


    /**********************************************************************************************************************************/
        /*private helper methods*/

        private class AsynchIndexRowReader implements Callable<Boolean> {
        private int numQueuedThreads = 0;

        public AsynchIndexRowReader() {
        }

        @Override
        public Boolean call() throws Exception {
            while (sourceIterator.hasNext())
                getMoreData();

            doneReading = true;
            // Add an empty LookupResponse so the last take() call doesn't hang.
            currentResultsQueue.add(new LookupResponse(new ArrayList<>(), -1));
            return true;
        }

        private void getMoreData() throws StandardException, IOException {
            if (sourceIterator.hasNext() && numQueuedThreads < numConcurrentLookups) {
                //read up to batchSize rows from the source, then submit them to the background thread for processing
                List<Pair<byte[], ExecRow>> sourceRows = Lists.newArrayListWithCapacity(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    if (!sourceIterator.hasNext())
                        break;
                    ExecRow next = sourceIterator.next();
                    for (int index = 0; index < indexCols.length; index++) {
                        if (indexCols[index] != -1) {
                            outputTemplate.setColumn(index + 1, next.getColumn(indexCols[index] + 1));
                        }
                    }
                    HBaseRowLocation rl = (HBaseRowLocation) next.getColumn(next.nColumns());
                    sourceRows.add(new Pair(rl.getBytes(), outputTemplate.getClone()));
                }
                if (!sourceRows.isEmpty()) {
                    //submit to the background thread
                    //boolean useOldIndexLookupMethod = getCompilerContext()
                    Lookup task = new Lookup(sourceRows, stackOfThreadNumbers.pop());
                    if (useOldIndexLookupMethod)
                        resultFutures.add(SIDriver.driver().getExecutorService().submit(task));
                    else
                        resultFutures.add(executorCompletionService.submit(task));
                    numQueuedThreads++;
                }

                //if there is only one submitted future, call this again to set off an additional background process
                //if (resultFutures.size() < numConcurrentLookups && sourceRows.size()==batchSize)
                //if (resultFutures.size() < numConcurrentLookups && sourceIterator.hasNext()) // msirek-temp
                //if(resultFutures.size()<numBlocks && sourceRows.size()==batchSize) msirek-temp
                if (numQueuedThreads < numConcurrentLookups && sourceIterator.hasNext()) // msirek-temp
                    getMoreData();
            }
            //else if(!resultFutures.isEmpty()){
            if (numQueuedThreads > 0) {  //msirek-temp
                waitForBlockCompletion();
            }
        }

        private void waitForBlockCompletion() throws StandardException, IOException {
            //wait for the first future to return correctly or error-out
            try {
                LookupResponse response;
                Future<LookupResponse> future = null;
                if (useOldIndexLookupMethod)
                    future = resultFutures.remove(0);
                else
                    future = executorCompletionService.take();
                response = future.get();
                response.yieldThreadNumber();
                IndexRowReader.this.addResultsToQueue(response);
                numQueuedThreads--;

            } catch (InterruptedException e) {
                throw new InterruptedIOException(e.getMessage());
            } catch (ExecutionException e) {
                Throwable t = e.getCause();
                if (t instanceof IOException) throw (IOException) t;
                else throw Exceptions.parseException(t);
            }
        }
    }


    public class LookupResponse {
        private final List<ExecRow> sourceRows;
        private int threadNumber;

        public LookupResponse(List<ExecRow> sourceRows, int threadNumber){
            this.sourceRows=sourceRows;
            this.threadNumber = threadNumber;
        }

        public boolean isEmpty() {
            return sourceRows.isEmpty();
        }

        public ExecRow getNextRow() {
            if (sourceRows.isEmpty()) {
                yieldThreadNumber();
                return null;
            }
            else
                return sourceRows.remove(0);
        }

        // Give up reservation of the held thread number so a new
        // thread can grab it.
        public void yieldThreadNumber() {
            if (threadNumber >= 0) {
                stackOfThreadNumbers.push(threadNumber);
                threadNumber = -1;
            }
        }
    }

    public class Lookup implements Callable<LookupResponse>{
        private final List<Pair<byte[],ExecRow>> sourceRows;
        private EntryDecoder decoder;
        private final int threadNumber;

        public Lookup(List<Pair<byte[],ExecRow>> sourceRows, int threadNumber){
            this.sourceRows=sourceRows;
            this.threadNumber = threadNumber;
        }

        @Override
        public LookupResponse call() throws Exception{
            Pair<ExecRow, DataResult> next;
            ExecRow nextScannedRow;
            DataResult nextFetchedData;
            byte[] rowKey;
            KeyDecoder keyDecoder = keyDecoders[threadNumber];
            KeyHashDecoder rowDecoder = rowDecoders[threadNumber];

            if (decoder == null)
                decoder = new EntryDecoder();

            List<byte[]> rowKeys = new ArrayList<>(sourceRows.size());
            for(Pair<byte[],ExecRow> sourceRow : sourceRows){
                rowKeys.add(sourceRow.getFirst());
            }
            Attributable attributable = new MapAttributes();
            attributable.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL,predicateFilterBytes);
            operationFactory.encodeForReads(attributable,txn,false);

            try(Partition table = tableFactory.getTable(Long.toString(mainTableConglomId))){
                Iterator<DataResult> results=table.batchGet(attributable,rowKeys);
                List<ExecRow> baseTableRows=Lists.newArrayListWithCapacity(sourceRows.size());
                for(Pair<byte[],ExecRow> sourceRow : sourceRows){
                    if(!results.hasNext())
                        throw new IllegalStateException("Programmer error: incompatible iterator sizes!");
                    next = Pair.newPair(sourceRow.getSecond(),results.next().getClone());
                    nextScannedRow = next.getFirst();
                    nextFetchedData=next.getSecond();
                    rowKey = nextFetchedData.key();
                    for(DataCell kv : nextFetchedData){
                        keyDecoder.decode(kv.keyArray(),kv.keyOffset(),kv.keyLength(),nextScannedRow);
                        rowDecoder.set(kv.valueArray(),kv.valueOffset(),kv.valueLength());
                        rowDecoder.decode(nextScannedRow);
                    }
                    nextScannedRow.setKey(rowKey);
                    baseTableRows.add(nextScannedRow);
                }
                return new LookupResponse(baseTableRows, threadNumber);
            }
        }
    }

    @Override
    public Iterator<ExecRow> iterator(){
        return IteratorUtils.asInterruptibleIterator(this);
    }

}
