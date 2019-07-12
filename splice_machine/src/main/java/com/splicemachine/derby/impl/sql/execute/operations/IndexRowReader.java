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

import com.splicemachine.EngineDriver;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.stream.function.IteratorUtils;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.TaskContext;
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
import scala.collection.JavaConverters;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
    private final int numBlocks;
    private final ExecRow outputTemplate;
    private final long mainTableConglomId;
    private final byte[] predicateFilterBytes;
    private final KeyDecoder keyDecoder;
    private final int[] indexCols;
    private final KeyHashDecoder rowDecoder;
    private final TxnView txn;
    private final TxnOperationFactory operationFactory;
    private final PartitionFactory tableFactory;
    private boolean init = false;

    private List<Pair<ExecRow, DataResult>> currentResults;
    private ArrayBlockingQueue<Future<List<Pair<ExecRow, DataResult>>>> resultFutures;
    private ArrayBlockingQueue<ExecRow> toReturn;
    protected Iterator<ExecRow> sourceIterator;

    private ExecRow heapRowToReturn;
    private ExecRow indexRowToReturn;

    IndexRowReader(Iterator<ExecRow> sourceIterator,
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
        this.sourceIterator=sourceIterator;
        this.outputTemplate=outputTemplate;
        this.txn=txn;
        batchSize=lookupBatchSize;
        this.numBlocks=Math.max(numConcurrentLookups, 2);
        this.mainTableConglomId=mainTableConglomId;
        this.predicateFilterBytes=predicateFilterBytes;
        this.tableFactory=tableFactory;
        this.keyDecoder=new KeyDecoder(keyDecoder,0);
        this.rowDecoder=rowDecoder;
        this.indexCols=indexCols;
        this.resultFutures = new ArrayBlockingQueue<>(this.numBlocks - 1);
        this.toReturn = new ArrayBlockingQueue<>(1024);
        this.operationFactory = operationFactory;
    }

    // Return the maximum number of threads that could be simultaneously
    // doing base conglomerate row lookups.
    public int getMaxConcurrency() {return this.numBlocks;}

    public void close() throws IOException{
        rowDecoder.close();
        keyDecoder.close();
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

    private Thread decoder;
    private Thread fetcher;
    private volatile Exception asyncException = null;
    private static ExecRow SENTINEL = new ValueRow();
    
    @Override
    public boolean hasNext(){
        if (!init) {
            init = true;
            fetcher = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        getMoreData();
                    } catch (Exception e) {
                        asyncException = e;
                        toReturn.offer(SENTINEL); // unblock main thread
                    }
                }
            }, "index-fetcher");
            fetcher.setDaemon(true);
            fetcher.start();
            decoder = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        boolean halt = false;
                        while (!halt) {
                            List<Pair<ExecRow, DataResult>> results = resultFutures.take().get();
                            if (results.size() < batchSize) {
                                // last batch, we must halt
                                halt = true;
                            }

                            for (Pair<ExecRow, DataResult> next : results) {
                                ExecRow nextScannedRow=next.getFirst();
                                DataResult nextFetchedData=next.getSecond();
                                byte[] rowKey = nextFetchedData.key();
                                for(DataCell kv : nextFetchedData){
                                    keyDecoder.decode(kv.keyArray(),kv.keyOffset(),kv.keyLength(),nextScannedRow);
                                    rowDecoder.set(kv.valueArray(),kv.valueOffset(),kv.valueLength());
                                    rowDecoder.decode(nextScannedRow);
                                }
                                nextScannedRow.setKey(rowKey);
                                toReturn.put(nextScannedRow);
                            }
                        }
                        toReturn.put(SENTINEL); // signal we are finished
                    } catch (Exception e) {
                        asyncException = e;
                        toReturn.offer(SENTINEL); // unblock main thread
                    }
                }
            },"index-decoder");
            decoder.setDaemon(true);
            decoder.start();
        }
        try{
            ExecRow nextScannedRow = toReturn.take();
            if ( nextScannedRow == SENTINEL) {
                heapRowToReturn = null;
                indexRowToReturn = null;
                if (asyncException != null)
                    throw asyncException;
                
                return false;
            } else {
                heapRowToReturn = nextScannedRow;
                indexRowToReturn = nextScannedRow;
                if (asyncException != null)
                    throw asyncException;

                return true;
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }


    /**********************************************************************************************************************************/
        /*private helper methods*/
    private void getMoreData() throws StandardException, IOException, InterruptedException {
        //read up to batchSize rows from the source, then submit them to the background thread for processing
        List<Pair<byte[],ExecRow>> sourceRows=Lists.newArrayListWithCapacity(batchSize);
        for(int i=0;i<batchSize;i++){
            if(!sourceIterator.hasNext())
                break;
            ExecRow next=sourceIterator.next();
            for(int index=0;index<indexCols.length;index++){
                if(indexCols[index]!=-1){
                    outputTemplate.setColumn(index+1,next.getColumn(indexCols[index]+1));
                }
            }
            HBaseRowLocation rl=(HBaseRowLocation)next.getColumn(next.nColumns());
            sourceRows.add(new Pair(rl.getBytes(), outputTemplate.getClone()));
        }
        //submit to the background thread
        Lookup task=new Lookup(sourceRows);
        resultFutures.put(SIDriver.driver().getExecutorService().submit(task));

        //if there is only one submitted future, call this again to set off an additional background process
        if(sourceRows.size()==batchSize)
            getMoreData();
    }

    public class Lookup implements Callable<List<Pair<ExecRow, DataResult>>>{
        private final List<Pair<byte[],ExecRow>> sourceRows;

        public Lookup(List<Pair<byte[],ExecRow>> sourceRows){
            this.sourceRows=sourceRows;
        }

        @Override
        public List<Pair<ExecRow, DataResult>> call() throws Exception{
            if (sourceRows.isEmpty()) return Collections.emptyList();
            
            List<byte[]> rowKeys = new ArrayList<>(sourceRows.size());
            for(Pair<byte[],ExecRow> sourceRow : sourceRows){
                rowKeys.add(sourceRow.getFirst());
            }
            Attributable attributable = new MapAttributes();
            attributable.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL,predicateFilterBytes);
            operationFactory.encodeForReads(attributable,txn,false);

            try(Partition table = tableFactory.getTable(Long.toString(mainTableConglomId))){
                Iterator<DataResult> results=table.batchGet(attributable,rowKeys);
                List<Pair<ExecRow, DataResult>> locations=Lists.newArrayListWithCapacity(sourceRows.size());
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
