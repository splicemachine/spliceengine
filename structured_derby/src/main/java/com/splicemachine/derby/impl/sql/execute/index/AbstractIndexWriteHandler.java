package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.batch.WriteHandler;
import com.splicemachine.hbase.writer.*;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.log4j.Logger;
import org.jruby.util.collections.IntHashMap;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
abstract class AbstractIndexWriteHandler extends SpliceConstants implements WriteHandler {
    /*
    * Maps the columns in the index to the columns in the main table.
    * e.g. if indexColsToMainColMap[0] = 1, then the first entry
    * in the index is the second column in the main table, and so on.
    */
//    protected final int[] indexColsToMainColMap;
    /*
     * A cache of column positions in the main table puts. This speeds
     * access and transformation of Puts and Deletes into Index Puts and
     * Deletes.
     */
//    protected final byte[][] mainColPos;
    /*
     * The id for the index table
     *
     * indexConglomBytes is a cached byte[] representation of the indexConglomId
     * to speed up transformations.
     */
    protected final byte[] indexConglomBytes;
    private static final Logger LOG = Logger.getLogger(AbstractIndexWriteHandler.class);

    protected boolean failed;

    protected final ObjectObjectOpenHashMap<KVPair,KVPair> indexToMainMutationMap = ObjectObjectOpenHashMap.newInstance();

    /*
     * The columns in the main table which are indexed (ordered by the position in the main table).
     */
    protected final BitSet indexedColumns;

    /*
     * The columns in the index table (e.g. mainColToIndexPosMap[indexedColumns.get()])
     */
    protected final BitSet translatedIndexColumns;
    /*
     * Mapping between the position in the main column's data stream, and the position in the index
     * key. The length of this is the same as the number of columns in the main table, and if the
     * fields isn't in the index, then the value of this map should be -1.
     */
    protected final int[] mainColToIndexPosMap;

    protected final BitSet descColumns;
    protected final boolean keepState;

    protected AbstractIndexWriteHandler(BitSet indexedColumns,int[] mainColToIndexPosMap,byte[] indexConglomBytes,BitSet descColumns,boolean keepState) {
        this.indexedColumns = indexedColumns;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.indexConglomBytes = indexConglomBytes;
        this.descColumns = descColumns;
        this.keepState = keepState;

        this.translatedIndexColumns = new BitSet(indexedColumns.cardinality());
        for(int i=indexedColumns.nextSetBit(0);i>=0;i=indexedColumns.nextSetBit(i+1)){
            translatedIndexColumns.set(mainColToIndexPosMap[i]);
        }
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if(failed)
            ctx.notRun(mutation);
        else{
            boolean sendUp = updateIndex(mutation,ctx);
            if(sendUp){
                ctx.sendUpstream(mutation);
            }
        }
    }

    @Override
    public void finishWrites(WriteContext ctx) throws IOException {
        try{
            finish(ctx);
        }catch(Exception e){
            SpliceLogUtils.error(LOG,e);
            if(e instanceof WriteFailedException){
                WriteFailedException wfe = (WriteFailedException)e;
                Object[] buffer = indexToMainMutationMap.values;
                int size = indexToMainMutationMap.size();
                for (int i = 0; i<size; i++) {
                	ctx.failed((KVPair)buffer[i], WriteResult.failed(wfe.getMessage()));
                }
            }
            else throw new IOException(e); //something unexpected went bad, need to propagate
        }
    }

    protected abstract boolean updateIndex(KVPair mutation, WriteContext ctx);

    protected abstract void finish(WriteContext ctx) throws Exception;

    protected CallBuffer<KVPair> getWriteBuffer(final WriteContext ctx,int expectedSize) throws Exception {
        WriteCoordinator.PreFlushHook flushHook = new WriteCoordinator.PreFlushHook() {
            @Override
            public ObjectArrayList<KVPair> transform(ObjectArrayList<KVPair> buffer) throws Exception {
            	ObjectArrayList<KVPair> newList = ObjectArrayList.newInstance();
            	Object[] array = buffer.buffer;
            	int size = buffer.size();
            	for (int i = 0; i< size; i++) {
                    KVPair mainInput = indexToMainMutationMap.get((KVPair)array[i]);
                    if (ctx.canRun(mainInput))
                    	newList.add((KVPair)array[i]);
            	}
            	return newList;
            }
        };
        Writer.WriteConfiguration writeConfiguration = new Writer.WriteConfiguration() {
            @Override
            public int getMaximumRetries() {
                return SpliceConstants.numRetries;
            }

            @Override
            public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
                if(t instanceof RegionTooBusyException){
                    try {
                        Thread.sleep(2*getPause());
                    } catch (InterruptedException e) {
                        LOG.info("Interrupted while waiting due to a RegionTooBusyException",e);
                    }

                    return Writer.WriteResponse.RETRY;
                }
                if( t instanceof ConnectException
                 || t instanceof WrongRegionException
                 || t instanceof NotServingRegionException
                 || t instanceof IndexNotSetUpException){
                    return Writer.WriteResponse.RETRY;
                }else
                    return Writer.WriteResponse.THROW_ERROR;
            }

            @Override
            public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
                IntHashMap<WriteResult> failedRows = result.getFailedRows();
                boolean canRetry = true;
                boolean regionTooBusy = false;
                for(WriteResult writeResult: failedRows.values()){
                    if(!writeResult.canRetry()){
                        canRetry=false;
                        break;
                    }if(writeResult.getCode()== WriteResult.Code.REGION_TOO_BUSY)
                        regionTooBusy = true;
                }

                if(regionTooBusy){
                    try{
                        Thread.sleep(2*getPause());
                    } catch (InterruptedException e) {
                        LOG.info("Interrupted while waiting due to a RegionTooBusyException",e);
                    }
                    return Writer.WriteResponse.RETRY;
                }
                if(canRetry) return Writer.WriteResponse.RETRY;
                else{
                    ObjectArrayList<KVPair> indexMutations = request.getMutations();
                    for(Integer row:failedRows.keySet()){
                        KVPair kvPair = indexMutations.get(row);
                        ctx.failed(indexToMainMutationMap.get(kvPair),failedRows.get(row));
                    }
                    return Writer.WriteResponse.IGNORE;
                }
            }

            @Override
            public long getPause() {
                return SpliceConstants.pause;
            }

						@Override
						public void writeComplete(long timeTakenMs, long numRecordsWritten) {
						}
				};

        return ctx.getWriteBuffer(indexConglomBytes,flushHook, writeConfiguration,expectedSize*2+10); //make sure we don't flush before we can
    }

    protected void accumulate(EntryAccumulator newKeyAccumulator, BitIndex updateIndex, ByteBuffer newBuffer, int newPos) {
        if(updateIndex.isScalarType(newPos))
            newKeyAccumulator.addScalar(mainColToIndexPosMap[newPos],newBuffer);
        else if(updateIndex.isFloatType(newPos))
            newKeyAccumulator.addFloat(mainColToIndexPosMap[newPos],newBuffer);
        else if(updateIndex.isDoubleType(newPos))
            newKeyAccumulator.addDouble(mainColToIndexPosMap[newPos],newBuffer);
        else
            newKeyAccumulator.add(mainColToIndexPosMap[newPos],newBuffer);
    }

    protected ByteBuffer getDescendingBuffer(ByteBuffer entry) {
        if(entry==null) return null;
        entry.mark();
        byte[] data = new byte[entry.remaining()];
        entry.get(data);
        entry.reset();
        for(int i=0;i<data.length;i++){
            data[i]^=0xff;
        }
        return ByteBuffer.wrap(data);
    }
}
