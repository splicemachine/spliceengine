/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
    private final ExecutorService lookupService;
    private final int batchSize;
    private final ExecRow outputTemplate;
    private final long mainTableConglomId;
    private final byte[] predicateFilterBytes;
    private final KeyDecoder keyDecoder;
    private final int[] indexCols;
    private final KeyHashDecoder rowDecoder;
    private final TxnView txn;
    private final TxnOperationFactory operationFactory;
    private final PartitionFactory tableFactory;
    private boolean hasMore = true;

    private List<Pair<ExecRow, DataResult>> currentResults;
    private BlockingQueue<Future<List<Pair<ExecRow, DataResult>>>> resultFutures;
    private EntryDecoder entryDecoder;
    protected Iterator<ExecRow> sourceIterator;

    private ExecRow heapRowToReturn;
    private ExecRow indexRowToReturn;

    IndexRowReader(ExecutorService lookupService,
                   Iterator<ExecRow> sourceIterator,
                   ExecRow outputTemplate,
                   TxnView txn,
                   int lookupBatchSize,
                   int numConcurrentLookups,
                   long mainTableConglomId,
                   byte[] predicateFilterBytes,
                   KeyHashDecoder keyDecoder,
                   KeyHashDecoder rowDecoder,
                   int[] indexCols,
                   TxnOperationFactory operationFactory,
                   PartitionFactory tableFactory){
        this.lookupService=lookupService;
        this.sourceIterator=sourceIterator;
        this.outputTemplate=outputTemplate;
        this.txn=txn;
        batchSize=lookupBatchSize;
        this.mainTableConglomId=mainTableConglomId;
        this.predicateFilterBytes=predicateFilterBytes;
        this.tableFactory=tableFactory;
        this.keyDecoder=new KeyDecoder(keyDecoder,0);
        this.rowDecoder=rowDecoder;
        this.indexCols=indexCols;
        this.resultFutures=new LinkedTransferQueue<>();
        this.operationFactory = operationFactory;
        Reader reader = new Reader();
        lookupService.submit(reader);
    }

    public void close() throws IOException{
        rowDecoder.close();
        keyDecoder.close();
        if(entryDecoder!=null)
            entryDecoder.close();
        lookupService.shutdownNow();
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
            if (!hasMore)
                return hasMore;

            if(currentResults==null || currentResults.size()<=0)
                getMoreData();

            if(currentResults==null || currentResults.size()<=0){
                hasMore = false;
                return hasMore; // No More Data
            }

            Pair<ExecRow, DataResult> next=currentResults.remove(0);
            //merge the results
            ExecRow nextScannedRow=next.getFirst();
            DataResult nextFetchedData=next.getSecond();
            byte[] rowKey = nextFetchedData.key();
            if(entryDecoder==null)
                entryDecoder=new EntryDecoder();
            for(DataCell kv : nextFetchedData){
                keyDecoder.decode(kv.keyArray(),kv.keyOffset(),kv.keyLength(),nextScannedRow);
                rowDecoder.set(kv.valueArray(),kv.valueOffset(),kv.valueLength());
                rowDecoder.decode(nextScannedRow);
            }
            nextScannedRow.setKey(rowKey);
            heapRowToReturn=nextScannedRow;
            indexRowToReturn=nextScannedRow;
            return true;
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }


    /**********************************************************************************************************************************/
        /*private helper methods*/
    private void getMoreData() throws StandardException, IOException{
        waitForBlockCompletion();
    }

    private void waitForBlockCompletion() throws StandardException, IOException{
        //wait for the first future to return correctly or error-out
        try{
            Future<List<Pair<ExecRow, DataResult>>> future=resultFutures.take();
            currentResults=future.get();
        }catch(InterruptedException e){
            throw new InterruptedIOException(e.getMessage());
        }catch(ExecutionException e){
            Throwable t=e.getCause();
            if(t instanceof IOException) throw (IOException)t;
            else throw Exceptions.parseException(t);
        }
    }

    public class Reader implements Callable<Void> {
        @Override
        public Void call() throws Exception {

            while (sourceIterator.hasNext()) {
                List<Pair<byte[],ExecRow>> sourceRows=Lists.newArrayListWithCapacity(batchSize);
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
                if (sourceRows.size() > 0) {
                    //submit to the background thread
                    Lookup task = new Lookup(sourceRows);
                    resultFutures.put(lookupService.submit(task));
                }
            }

            // Add an empty batch to mark the end
            List<Pair<byte[],ExecRow>> sourceRows=Lists.newArrayListWithCapacity(batchSize);
            Lookup task = new Lookup(sourceRows);
            resultFutures.put(lookupService.submit(task));
            return null;
        }
    }

    public class Lookup implements Callable<List<Pair<ExecRow, DataResult>>>{
        private final List<Pair<byte[],ExecRow>> sourceRows;

        public Lookup(List<Pair<byte[],ExecRow>> sourceRows){
            this.sourceRows=sourceRows;
        }

        @Override
        public List<Pair<ExecRow, DataResult>> call() throws Exception{
            List<Pair<ExecRow, DataResult>> locations=Lists.newArrayListWithCapacity(sourceRows.size());
            if(sourceRows.size() == 0)
                return locations;

            List<byte[]> rowKeys = new ArrayList<>(sourceRows.size());
            for(Pair<byte[],ExecRow> sourceRow : sourceRows){
                rowKeys.add(sourceRow.getFirst());
            }
            Attributable attributable = new MapAttributes();
            attributable.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL,predicateFilterBytes);
            operationFactory.encodeForReads(attributable,txn,false);

            try(Partition table = tableFactory.getTable(Long.toString(mainTableConglomId))){
                Iterator<DataResult> results=table.batchGet(attributable,rowKeys);

                for(Pair<byte[],ExecRow> sourceRow : sourceRows){
                    if(!results.hasNext())
                        throw new IllegalStateException("Programmer error: incompatible iterator sizes!");
                    locations.add(Pair.newPair(sourceRow.getSecond(),results.next().getClone()));
                }
                return locations;
            }
        }
    }

    @Override
    public Iterator<ExecRow> iterator(){
        return IteratorUtils.asInterruptibleIterator(this);
    }

}
