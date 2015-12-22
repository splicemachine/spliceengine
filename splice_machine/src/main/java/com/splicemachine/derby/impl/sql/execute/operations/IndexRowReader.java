package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.KeyDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Iterator;
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
public class IndexRowReader implements Iterator<LocatedRow>, Iterable<LocatedRow> {
    protected static Logger LOG = Logger.getLogger(IndexRowReader.class);
    private final ExecutorService lookupService;
    private final int batchSize;
	private final int numBlocks;
    private final ExecRow outputTemplate;
    private final long mainTableConglomId;
	private final byte[] predicateFilterBytes;
	private final KeyDecoder keyDecoder;
	private final int[] indexCols;
	private final KeyHashDecoder rowDecoder;
    private final TxnView txn;
    private List<Pair<LocatedRow,Result>> currentResults;
    private LocatedRow toReturn = new LocatedRow();
    private List<Future<List<Pair<LocatedRow,Result>>>> resultFutures;
    private boolean populated = false;
	private EntryDecoder entryDecoder;
    protected Iterator<LocatedRow> sourceIterator;

	IndexRowReader(ExecutorService lookupService,
									 Iterator<LocatedRow> sourceIterator,
									 ExecRow outputTemplate,
									 TxnView txn,
									 int lookupBatchSize,
									 int numConcurrentLookups,
									 long mainTableConglomId,
									 byte[] predicateFilterBytes,
									 KeyHashDecoder keyDecoder,
									 KeyHashDecoder rowDecoder,
									 int[] indexCols) {
				this.lookupService = lookupService;
				this.sourceIterator = sourceIterator;
				this.outputTemplate = outputTemplate;
                this.txn = txn;
				batchSize = lookupBatchSize;
				this.numBlocks = numConcurrentLookups;
				this.mainTableConglomId = mainTableConglomId;
				this.predicateFilterBytes = predicateFilterBytes;
				this.keyDecoder = new KeyDecoder(keyDecoder,0);
				this.rowDecoder = rowDecoder;
				this.indexCols = indexCols;
				this.resultFutures = Lists.newArrayListWithCapacity(numConcurrentLookups);
		}

    public void close() throws IOException {
				Closeables.closeQuietly(this.rowDecoder);
				Closeables.closeQuietly(this.keyDecoder);
				if(entryDecoder!=null)
            entryDecoder.close();
        lookupService.shutdownNow();
    }

    @Override
    public LocatedRow next() {
        return toReturn;
    }

    @Override
    public void remove() {

    }

    @Override
    public boolean hasNext() {
        try {
            if (currentResults == null || currentResults.size() <= 0)
                getMoreData();

            if (currentResults == null || currentResults.size() <= 0)
                return false; // No More Data
            Pair<LocatedRow, Result> next = currentResults.remove(0);
            //merge the results
            LocatedRow nextScannedRow = next.getFirst();
            Result nextFetchedData = next.getSecond();
            if (entryDecoder == null)
                entryDecoder = new EntryDecoder();
            for (KeyValue kv : nextFetchedData.raw()) {
                byte[] buffer = kv.getBuffer();
                keyDecoder.decode(buffer, kv.getRowOffset(), kv.getRowLength(), nextScannedRow.getRow());
                rowDecoder.set(buffer, kv.getValueOffset(), kv.getValueLength());
                rowDecoder.decode(nextScannedRow.getRow());
            }
            toReturn = nextScannedRow;
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


/**********************************************************************************************************************************/
		/*private helper methods*/
    private void getMoreData() throws StandardException, IOException {
        //read up to batchSize rows from the source, then submit them to the background thread for processing
        List<LocatedRow> sourceRows = Lists.newArrayListWithCapacity(batchSize);
        for(int i=0;i<batchSize;i++){
            if (!sourceIterator.hasNext())
                break;
            LocatedRow next = sourceIterator.next();
            for(int index=0;index<indexCols.length;index++){
                if(indexCols[index]!=-1){
                    outputTemplate.setColumn(index + 1, next.getRow().getColumn(indexCols[index] + 1));
                }
            }
            sourceRows.add(new LocatedRow((RowLocation) next.getRow().getColumn(next.getRow().nColumns()),
                    outputTemplate.getClone()));
        }
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

        private class Lookup implements Callable<List<Pair<LocatedRow,Result>>> {
            private final List<LocatedRow> sourceRows;
            private Table table;

            public Lookup(List<LocatedRow> sourceRows) {
                this.sourceRows = sourceRows;
            }

            @Override
            public List<Pair<LocatedRow,Result>> call() throws Exception {
                    List<Get> gets = Lists.newArrayListWithCapacity(sourceRows.size());
            for(LocatedRow sourceRow:sourceRows){
                byte[] row = sourceRow.getRowLocation().getBytes();
                Get get = SpliceUtils.createGet(txn,row);
                get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilterBytes);
                gets.add(get);
            }
            try {
                table = SpliceAccessManager.getHTable(mainTableConglomId);
                Result[] results = table.get(gets);
                int i = 0;
                List<Pair<LocatedRow, Result>> locations = Lists.newArrayListWithCapacity(sourceRows.size());
                for (LocatedRow sourceRow : sourceRows) {
                    locations.add(Pair.newPair(sourceRow, results[i]));
                    i++;
                }
                return locations;
            } finally {
                if (table != null)
                    table.close();
            }
        }
    }

    @Override
    public Iterator<LocatedRow> iterator() {
        return this;
    }

}
