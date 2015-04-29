package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.KeyDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.metrics.*;
import com.splicemachine.hbase.HBaseStatUtils;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Utility for executing "look-ahead" index lookups, where the index lookup is backgrounded,
 * while other processes occur on the caller thread.
 *
 * @author Scott Fines
 * Created on: 9/4/13
 */
class IndexRowReader {
    protected static Logger LOG = Logger.getLogger(IndexRowReader.class);

    private final ExecutorService lookupService;
    private final SpliceOperation sourceOperation;
    private final int batchSize;
		private final int numBlocks;
    private final ExecRow outputTemplate;
    private final long mainTableConglomId;
		private final byte[] predicateFilterBytes;
		private final MetricFactory metricFactory;
		private final SpliceRuntimeContext runtimeContext;

		private final KeyDecoder keyDecoder;
		private final int[] indexCols;
		private final KeyHashDecoder rowDecoder;
    private final TxnView txn;

    private List<Pair<RowAndLocation,Result>> currentResults;
    private RowAndLocation toReturn = new RowAndLocation();
    private List<Future<List<Pair<RowAndLocation,Result>>>> resultFutures;

    private boolean populated = false;
		private HTableInterface table;

		private EntryDecoder entryDecoder;

		IndexRowReader(ExecutorService lookupService,
									 SpliceOperation sourceOperation,
									 ExecRow outputTemplate,
									 TxnView txn,
									 int lookupBatchSize,
									 int numConcurrentLookups,
									 long mainTableConglomId,
									 byte[] predicateFilterBytes,
									 MetricFactory metricFactory,
									 SpliceRuntimeContext runtimeContext,
									 KeyHashDecoder keyDecoder,
									 KeyHashDecoder rowDecoder,
									 int[] indexCols, HTableInterface table) {
				this.lookupService = lookupService;
				this.sourceOperation = sourceOperation;
				this.outputTemplate = outputTemplate;
        this.txn = txn;
				batchSize = lookupBatchSize;
				this.numBlocks = numConcurrentLookups;
				this.mainTableConglomId = mainTableConglomId;
				this.predicateFilterBytes = predicateFilterBytes;
				this.metricFactory = metricFactory;
				this.runtimeContext = runtimeContext;
				this.keyDecoder = new KeyDecoder(keyDecoder,0);
				this.rowDecoder = rowDecoder;
				this.indexCols = indexCols;
				this.resultFutures = Lists.newArrayListWithCapacity(numConcurrentLookups);
				this.table =table;
		}

    public void close() throws IOException {
				Closeables.closeQuietly(this.rowDecoder);
				Closeables.closeQuietly(this.keyDecoder);
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
            entryDecoder = new EntryDecoder();

        for(KeyValue kv:nextFetchedData.raw()){
						byte[] buffer = kv.getBuffer();
						keyDecoder.decode(buffer,kv.getRowOffset(),kv.getRowLength(),nextScannedRow.row);
						rowDecoder.set(buffer,kv.getValueOffset(),kv.getValueLength());
						rowDecoder.decode(nextScannedRow.row);
        }

        return nextScannedRow;
    }

		public long getTotalRows(){
				/*
				 * We do the type checking like this (even though it's ugly), so that
				 * we can easily swap between a NoOpMetricFactory and an AtomicTimer without
				 * paying a significant penalty in if-branches (lots of if(recordStats) blocks)
				 *
				 * In general, this will only be called if record stats is true, but we put
				 * in the instanceof check just to be safe
				 */
				if(metricFactory instanceof AtomicTimer)
						return ((AtomicTimer)metricFactory).getTotalEvents();
				return 0;
		}

		public TimeView getTimeInfo(){
				if(metricFactory instanceof AtomicTimer)
						return ((AtomicTimer)metricFactory).getTimeView();
				return Metrics.noOpTimeView();
		}

		public long getBytesFetched(){
				if(metricFactory instanceof AtomicTimer)
						return ((AtomicTimer)metricFactory).getTotalCountedValues();
				return 0;
		}

/**********************************************************************************************************************************/
		/*private helper methods*/
    private void getMoreData() throws StandardException, IOException {
        //read up to batchSize rows from the source, then submit them to the background thread for processing
        List<RowAndLocation> sourceRows = Lists.newArrayListWithCapacity(batchSize);
        for(int i=0;i<batchSize;i++){
            ExecRow next = sourceOperation.getNextRowCore();
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

        if(table==null)
            table = SpliceAccessManager.getHTable(mainTableConglomId);

        if(sourceRows.size()>0){
            //submit to the background thread
            resultFutures.add(lookupService.submit(new Lookup(sourceRows)));
        }

        //if there is only one submitted future, call this again to set off an additional background process
        if(resultFutures.size()<numBlocks && sourceRows.size()==batchSize)
            getMoreData();
        else if(resultFutures.size()>0){
            waitForBlockCompletion();
        }
    }

    private void waitForBlockCompletion() throws StandardException,IOException {
        //wait for the first future to return correctly or error-out
        try {
            currentResults = resultFutures.remove(0).get();
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if(t instanceof IOException) throw (IOException)t;
            else throw Exceptions.parseException(t);
        }
    }
    /** TODO REMOVE JLEACH */
    static class RowAndLocation{
        ExecRow row;
        byte[] rowLocation;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RowAndLocation)) return false;

            RowAndLocation that = (RowAndLocation) o;

            if(!Bytes.equals(rowLocation,that.rowLocation)) return false;

            if(row.nColumns()!=that.row.nColumns()) return false;
            DataValueDescriptor[] leftRow = row.getRowArray();
            DataValueDescriptor[] rightRow = row.getRowArray();
            for(int i=0;i<leftRow.length;i++){
                if(!leftRow[i].equals(rightRow[i])) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = row.hashCode();
            result = 31 * result + Arrays.hashCode(rowLocation);
            return result;
        }
    }

    private class Lookup implements Callable<List<Pair<RowAndLocation,Result>>> {
        private final List<RowAndLocation> sourceRows;
				private final Timer timer;
				private final Counter bytesCounter;

        public Lookup(List<RowAndLocation> sourceRows) {
            this.sourceRows = sourceRows;
						this.timer = metricFactory.newTimer();
						this.bytesCounter = metricFactory.newCounter();
        }

        @Override
        public List<Pair<RowAndLocation,Result>> call() throws Exception {
						List<Get> gets = Lists.newArrayListWithCapacity(sourceRows.size());
						for(RowAndLocation sourceRow:sourceRows){
								byte[] row = sourceRow.rowLocation;
								Get get = SpliceUtils.createGet(txn,row);
								get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilterBytes);
								gets.add(get);
						}
						timer.startTiming();
						Result[] results = table.get(gets);
						timer.tick(results.length);
						HBaseStatUtils.countBytes(bytesCounter, results);
						int i=0;
						List<Pair<RowAndLocation,Result>> locations = Lists.newArrayListWithCapacity(sourceRows.size());
						for(RowAndLocation sourceRow:sourceRows){
								locations.add(Pair.newPair(sourceRow,results[i]));
								i++;
						}
						return locations;
				}
    }
}
