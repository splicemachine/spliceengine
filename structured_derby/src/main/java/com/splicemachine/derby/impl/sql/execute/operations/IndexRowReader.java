package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.KeyDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.stats.*;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Arrays;
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
    protected static Logger LOG = Logger.getLogger(IndexRowReader.class);

    private final ExecutorService lookupService;
    private final SpliceOperation sourceOperation;
    private final int batchSize;
    private final int numBlocks;
    private final ExecRow outputTemplate;
    private final String txnId;
    private final int[] indexCols;
    private final long mainTableConglomId;
		private final byte[] predicateFilterBytes;

    private List<Pair<RowAndLocation,Result>> currentResults;
    private RowAndLocation toReturn = new RowAndLocation();
    private List<Future<List<Pair<RowAndLocation,Result>>>> resultFutures;

    private HTableInterface table;
    private boolean populated = false;

		private EntryDecoder entryDecoder;
		private MetricFactory metricFactory;
		private SpliceRuntimeContext runtimeContext;

//		KeyMarshaller keyMarshaller;
//		MultiFieldDecoder keyDecoder;
//		DataValueDescriptor[] kdvds;

		KeyDecoder keyDecoder;
		KeyHashDecoder rowDecoder;

		IndexRowReader(ExecutorService lookupService,
									 HTableInterface table,
									 SpliceOperation sourceOperation,
									 int batchSize,
									 int numBlocks,
									 ExecRow template,
									 String txnId,
									 int[] indexCols,
									 long mainTableConglomId,
									 int[] adjustedBaseColumnMap,
									 byte[] predicateFilterBytes,
									 SpliceRuntimeContext runtimeContext,
									 int[] mainTablePkCols,
									 String tableVersion) throws StandardException{
				this.lookupService = lookupService;
				this.sourceOperation = sourceOperation;
				this.batchSize = batchSize;
				this.numBlocks = numBlocks;
				this.outputTemplate = template;
				this.txnId = txnId;
				this.indexCols = indexCols;
				this.mainTableConglomId = mainTableConglomId;
				this.resultFutures = Lists.newArrayListWithExpectedSize(numBlocks);
				this.table = table;
				this.runtimeContext = runtimeContext;

        this.predicateFilterBytes = predicateFilterBytes;
        if(runtimeContext.shouldRecordTraceMetrics()){
            metricFactory = Metrics.atomicTimer();
        }else
            metricFactory = Metrics.noOpMetricFactory();
//        this.columnOrdering = columnOrdering;

				DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(template);
				if(mainTablePkCols==null||mainTablePkCols.length<=0)
						keyDecoder = KeyDecoder.noOpDecoder();
				else
						keyDecoder = KeyDecoder.bareDecoder(mainTablePkCols, serializers);
				rowDecoder = new EntryDataDecoder(adjustedBaseColumnMap,null, serializers);
//        if (columnOrdering != null) {
//            getColumnDVDs();
//        }
    }

    IndexRowReader(ExecutorService lookupService,
									 SpliceOperation sourceOperation,
									 int batchSize,
									 int numBlocks,
									 ExecRow template,
									 String txnId,
									 int[] indexCols,
									 long mainTableConglomId,
									 int[] adjustedBaseColumnMap,
									 byte[] predicateFilterBytes,
									 SpliceRuntimeContext runtimeContext,
									 int[] columnOrdering,
									 String tableVersion) throws StandardException{
        this(lookupService,null,sourceOperation,batchSize,numBlocks,template,
             txnId,indexCols,mainTableConglomId,adjustedBaseColumnMap,
             predicateFilterBytes,runtimeContext, columnOrdering,tableVersion);
    }

    public static IndexRowReader create(SpliceOperation sourceOperation,
                                        long mainTableConglomId,
                                        ExecRow template,
                                        String txnId,
                                        int[] indexCols,
                                        int[] adjustedBaseColumnMap,
                                        FormatableBitSet heapOnlyCols,
										SpliceRuntimeContext runtimeContext,
                                        int[] columnOrdering,
                                        int[] format_ids,
																				String tableVersion) throws StandardException{
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
        EntryPredicateFilter epf = new EntryPredicateFilter(fieldsToReturn, new ObjectArrayList<Predicate>());
        byte[] predicateFilterBytes = epf.toBytes();
        return new IndexRowReader(backgroundService,
                sourceOperation,
                batchSize,numBlocks,
                template,txnId,
                indexCols,mainTableConglomId,
                adjustedBaseColumnMap,predicateFilterBytes,
                runtimeContext,
                columnOrdering,
								tableVersion
				);
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
            entryDecoder = new EntryDecoder(runtimeContext.getKryoPool());

        for(KeyValue kv:nextFetchedData.raw()){
						byte[] buffer = kv.getBuffer();
						keyDecoder.decode(buffer,kv.getRowOffset(),kv.getRowLength(),nextScannedRow.row);
						rowDecoder.set(buffer,kv.getValueOffset(),kv.getValueLength());
						rowDecoder.decode(nextScannedRow.row);
//            if (pkColumsAccessed()) {
//                getKeyMarshaller().decode(kv, nextScannedRow.row.getRowArray(), adjustedBaseColumnMap, getKeyDecoder(), columnOrdering, kdvds);
//            }
//            RowMarshaller.sparsePacked().decode(kv,nextScannedRow.row.getRowArray(),adjustedBaseColumnMap,entryDecoder);
        }

        return nextScannedRow;
    }

//    private KeyMarshaller getKeyMarshaller () {
//        if (keyMarshaller == null)
//            keyMarshaller = new KeyMarshaller();
//
//        return keyMarshaller;
//    }

//    private MultiFieldDecoder getKeyDecoder() {
//        if (keyDecoder == null)
//            keyDecoder = MultiFieldDecoder.create(SpliceDriver.getKryoPool());
//        return keyDecoder;
//    }
//
//    private void getColumnDVDs() throws StandardException{
//        if (kdvds == null) {
//            kdvds = new DataValueDescriptor[columnOrdering.length];
//            for (int i = 0; i < columnOrdering.length; ++i) {
//                kdvds[i] = LazyDataValueFactory.getLazyNull(format_ids[columnOrdering[i]]);
//            }
//        }
//    }

//    private boolean pkColumsAccessed() {
//        if (columnOrdering != null && columnOrdering.length > 0) {
//            for (int col:columnOrdering) {
//                if (col < adjustedBaseColumnMap.length && adjustedBaseColumnMap[col] > -1)
//                    return true;
//            }
//        }
//        return false;
//    }

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
            ExecRow next = sourceOperation.nextRow(runtimeContext);
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

    private void waitForBlockCompletion() throws StandardException {
        //wait for the first future to return correctly or error-out
        try {
            currentResults = resultFutures.remove(0).get();
        } catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e);
        }
    }

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
								Get get = new Get(row);
								get.setAttribute(SIConstants.SI_NEEDED,SIConstants.TRUE_BYTES);
								get.setAttribute(SIConstants.SI_TRANSACTION_ID_KEY,Bytes.toBytes(txnId));
								get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilterBytes);
								gets.add(get);
						}

						timer.startTiming();
						Result[] results = table.get(gets);
						timer.tick(results.length);
						StatUtils.countBytes(bytesCounter,results);

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
