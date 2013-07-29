package com.splicemachine.derby.impl.sql.execute.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.catalog.IndexDescriptor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Manages Index updates to keep it in sync with main table lookups.
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class IndexManager {
    private static final Logger LOG = Logger.getLogger(IndexManager.class);
    /*
     * Maps the columns in the index to the columns in the main table.
     * e.g. if indexColsToMainColMap[0] = 1, then the first entry
     * in the index is the second column in the main table, and so on.
     */
    private final int[] indexColsToMainColMap;

    /*
     * A cache of column positions in the main table puts. This speeds
     * access and transformation of Puts and Deletes into Index Puts and
     * Deletes.
     */
    private final byte[][] mainColPos;

    /*
     * The id for the index table
     *
     * indexConglomBytes is a cached byte[] representation of the indexConglomId
     * to speed up transformations.
     */
    private final long indexConglomId;
    private final byte[] indexConglomBytes;


    /*
     * Indicator that this index is unique. If it is not unique, then
     * the IndexManager must append a postfix on to the end of each row it creates.
     */
    private final boolean isUnique;

    private IndexManager(long indexConglomId, int[] baseColumnMap,boolean isUnique){
        this.indexColsToMainColMap = translate(baseColumnMap);
        this.indexConglomId = indexConglomId;
        this.indexConglomBytes = Long.toString(indexConglomId).getBytes();
        this.isUnique = isUnique;

        mainColPos = new byte[baseColumnMap.length][];
        for(int i=0;i<baseColumnMap.length;i++){
            mainColPos[i] = Encoding.encode(baseColumnMap[i] - 1);
        }
    }

    /**
     * Update the index to remain in sync with the specified main table mutation.
     *
     * @param mutation the main table mutation
     * @param rce the region environment
     * @throws IOException if something goes wrong updating the index
     */
    public void update(Mutation mutation, RegionCoprocessorEnvironment rce) throws IOException{
        /*
         * It turns out that rce.getTable(indexConglomBytes) is a really expensive call,
         * so it's best to avoid it whenever the mutation has already been managed externally.
         */
        if(mutation.getAttribute(IndexSet.INDEX_UPDATED)!=null) return; //index already managed for this
        try{
            CallBuffer<Mutation> writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(indexConglomBytes);
            update(mutation,writeBuffer,rce,null);
            writeBuffer.flushBuffer();
            writeBuffer.close();
        }catch(Exception e){
            throw Exceptions.getIOException(e);
        }
    }

    /**
     * Update the index to remain in sync with <em> all</em> the specified main table mutations.
     *
     * @param mutations the mutations to the main table to sync
     * @param rce the region environment
     * @throws IOException if something goes wrong syncing the index with <em>any</em> mutation in {@code mutations}
     */
    public void update(Collection<Mutation> mutations, RegionCoprocessorEnvironment rce) throws IOException{
        //get the table ahead of time for better batch putting

        try {
            CallBuffer<Mutation> writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(indexConglomBytes);

            HTableInterface deleteTable = null;
            for(Mutation mutation:mutations){
                deleteTable = update(mutation,writeBuffer,rce,deleteTable);
            }
            writeBuffer.flushBuffer();
            writeBuffer.close();
        } catch (Exception e) {
            throw Exceptions.getIOException(e);
        }
    }

    /**
     * Translate a list of KeyValue objects into a List of insert puts.
     *
     *
     * @param transactionId
     * @param result the keyvalues to translate
     * @return a list of puts, one for each distinct row in {@code result}
     * @throws IOException if something goes wrong during the translation
     */
    public List<Put> translateResult(String transactionId, List<KeyValue> result) throws IOException{
        Map<byte[],List<KeyValue>> putConstructors = Maps.newHashMapWithExpectedSize(1);
        for(KeyValue keyValue:result){
            List<KeyValue> cols = putConstructors.get(keyValue.getRow());
            if(cols==null){
                cols = Lists.newArrayListWithExpectedSize(indexColsToMainColMap.length);
                putConstructors.put(keyValue.getRow(),cols);
            }
            cols.add(keyValue);
        }
        //build Puts for each row
        List<Put> indexPuts = Lists.newArrayListWithExpectedSize(putConstructors.size());
        for(byte[] mainRow: putConstructors.keySet()){
            List<KeyValue> rowData = putConstructors.get(mainRow);
            byte[][] indexRowData = getDataArray();
            int rowSize=0;
            for(KeyValue kv:rowData){
                int colPos = Integer.parseInt(Bytes.toString(kv.getQualifier()));
                for(int indexPos=0;indexPos<indexColsToMainColMap.length;indexPos++){
                    if(colPos == indexColsToMainColMap[indexPos]){
                        byte[] val = kv.getValue();
                        indexRowData[indexPos] = val;
                        rowSize+=val.length;
                        break;
                    }
                }
            }
            if(!isUnique){
                byte[] postfix = SpliceUtils.getUniqueKey();
                indexRowData[indexRowData.length-1] = postfix;
                rowSize+=postfix.length;
            }

            byte[] finalIndexRow = new byte[rowSize];
            int offset =0;
            for(byte[] indexCol:indexRowData){
                System.arraycopy(indexCol,0,finalIndexRow,offset,indexCol.length);
                offset+=indexCol.length;
            }
            Put indexPut = SpliceUtils.createPut(finalIndexRow, transactionId);
            for(int dataPos=0;dataPos<indexRowData.length;dataPos++){
                byte[] putPos = Encoding.encode(dataPos);
                indexPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES,putPos,indexRowData[dataPos]);
            }

            indexPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES,Encoding.encode(rowData.size()),mainRow);
            indexPuts.add(indexPut);
        }

        return indexPuts;
    }

/*********************************************************************************************************************/
    /*private helper methods*/

    /*
     * Convenience wrapper around type casting of the mutation.
     */
    private HTableInterface update(Mutation mutation,
                        CallBuffer<Mutation> writeBuffer,
                        RegionCoprocessorEnvironment rce,HTableInterface table) throws Exception {
        if(Mutations.isDelete(mutation))
            return doDelete(mutation,writeBuffer,rce,table);
        else{
            updateIndex((Put) mutation, writeBuffer, rce.getRegion());
            return null;
        }
    }

    /*
     * convert a one-based int[] into a zero-based. In essence, shift all values in the array down by one
     */
    private static int[] translate(int[] ints) {
        int[] zeroBased = new int[ints.length];
        for(int pos=0;pos<ints.length;pos++){
            zeroBased[pos] = ints[pos]-1;
        }
        return zeroBased;
    }


    /*
     * Update the index to delete records that are no longer in the main table.
     */
    private HTableInterface doDelete(final Mutation mutation, CallBuffer<Mutation> writeBuffer,
                                   RegionCoprocessorEnvironment rce, HTableInterface table) throws Exception {
        /*
         * To delete an entry, we'll need to first get the row, then construct
         * the index row key from the row, then delete it
         */
        Get get = SpliceUtils.createGet(mutation, mutation.getRow());

        for(byte[] mainColumn:mainColPos){
            get.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,mainColumn);
        }
        Result result = rce.getRegion().get(get);
        if(result==null||result.isEmpty()) return table; //already deleted? weird, but oh well, we're good

        NavigableMap<byte[],byte[]> familyMap = result.getFamilyMap(SpliceConstants.DEFAULT_FAMILY_BYTES);
        byte[][] rowKeyBuilder = getDataArray();
        int size = 0;
        for(int indexPos=0;indexPos<indexColsToMainColMap.length;indexPos++){
            byte[] mainPutPos = mainColPos[indexPos];
            byte[] data = familyMap.get(mainPutPos);
            rowKeyBuilder[indexPos] = data;
            size+=data.length;
        }

        final byte[] indexRowKey = convert(rowKeyBuilder,size);

        if(isUnique){
            writeBuffer.add(Mutations.getDeleteOp(mutation, indexRowKey));
        }else{
            /*
             * Because index keys in non-null indices have a postfix appended to them,
             * we don't know exactly which row to delete, so we need to scan over the range
             * until we find the first row, then delete it. We can do this locally by pushing
             * to the BatchProtocol endpoint
             */
            final byte[] indexStop = BytesUtil.unsignedCopyAndIncrement(indexRowKey);
            try {
                if(table==null) table = rce.getTable(indexConglomBytes);
                table.coprocessorExec(BatchProtocol.class,indexRowKey,indexStop,new Batch.Call<BatchProtocol, Void>() {
                    @Override
                    public Void call(BatchProtocol instance) throws IOException {
                        instance.deleteFirstAfter(SpliceUtils.getTransactionId(mutation),indexRowKey,indexStop);
                        return null;
                    }
                });
            } catch (Throwable throwable) {
                if(throwable instanceof IOException) throw (IOException)throwable;
                throw new IOException(throwable);
            }
        }
        return table;
    }

    /*
     * concatenate all the entries in rowKeyBuilder into a single byte[] for keying off of
     */
    private byte[] convert(byte[][] rowKeyBuilder, int size) {
        byte[] indexRowKey = new byte[size];
        int offset = 0;
        boolean isStart=true;
        for(byte[] nextKey:rowKeyBuilder){
            if(nextKey==null) break;
            if(!isStart){
                indexRowKey[offset] = 1;
                offset++;
            }else
                isStart = false;

            System.arraycopy(nextKey, 0, indexRowKey, offset, nextKey.length);
            offset+=nextKey.length;
        }
        return indexRowKey;
    }

    /*
     * Update the index to manage inserts and updates.
     */
    private void updateIndex(Put mainPut,CallBuffer<Mutation>writeBuffer,HRegion region) throws Exception {
        Put put = doUpdate(mainPut,writeBuffer,region);
        if(put==null) return; //this was an update, but it doesn't affect our index, whoo!
        byte[][] rowKeyBuilder = getDataArray();
        int size=0;

        for(int indexPos=0;indexPos< indexColsToMainColMap.length;indexPos++){
            byte[] putPos = mainColPos[indexPos];
            byte[] data = put.get(SpliceConstants.DEFAULT_FAMILY_BYTES,putPos).get(0).getValue();
            rowKeyBuilder[indexPos] = data;
            size+=data.length+1;
        }
        if(!isUnique){
            byte[] postfix = SpliceUtils.getUniqueKey();
            rowKeyBuilder[rowKeyBuilder.length-1] = postfix;
            size+=postfix.length;
        }

        byte[] indexRowKey = convert(rowKeyBuilder,size);

        Put indexPut = SpliceUtils.createPut(indexRowKey, mainPut);
        for(int i=0;i<indexColsToMainColMap.length;i++){
            byte[] indexPos = Encoding.encode(i);
            indexPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES,indexPos,rowKeyBuilder[i]);
        }

        //add the put rowKey as the row location at the end of the row
        byte[] locPos = Encoding.encode(indexColsToMainColMap.length);

        indexPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES,locPos,put.getRow());

        doPut(indexPut, writeBuffer) ;
    }

    /*
     * Deal with Puts which are logged as Update-types.
     *
     * In HBase, Updates and Inserts are the same thing, but in Splice they are not. Thus, update operations
     * tag puts with an update tag, which this notices and performs the update to the index if necessary.
     *
     * If the update doesn't touch any of the indexed columns, then this is a no-op. Otherwise, a local get
     * is performed to get the old values of the index row, which is then deleted from the index. Then
     * this Put is treated as if it were an insert.
     */
    private Put doUpdate(Put mainPut, CallBuffer<Mutation> writeBuffer,HRegion region) throws Exception {
        if(!Bytes.equals(mainPut.getAttribute(Puts.PUT_TYPE),Puts.FOR_UPDATE))
            return mainPut; //it's an insert
        //check if we changed anything in the index
        boolean indexNeedsUpdating = false;
        for(byte[] indexColPo:mainColPos){
            if(mainPut.has(SpliceConstants.DEFAULT_FAMILY_BYTES,indexColPo)){
                indexNeedsUpdating = true;
                break;
            }
        }

        if(!indexNeedsUpdating) return null; //nothing changed that we indexed, whoo!

        //bummer, have to update the index
        Get oldGet = SpliceUtils.createGet(mainPut, mainPut.getRow());
        for(byte[] indexColPos:mainColPos){
            oldGet.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,indexColPos);
        }

        Result r = region.get(oldGet);
        if(r==null||r.isEmpty()) return mainPut; //no row to change, so this is really an insert!

        byte[][] rowToDelete = getDataArray();
        int size =0;
        for(int indexPos = 0;indexPos<mainColPos.length;indexPos++){
            byte[] data = r.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES,mainColPos[indexPos]);
            rowToDelete[indexPos] = data;
            size+=data.length+1;
        }

        byte[] indexRowKey = convert(rowToDelete,size);
        Mutation delete = Mutations.translateToDelete(mainPut,indexRowKey);
        writeBuffer.add(delete);

        //merge the old row with the new row to form the new index put
        Put newPut = Mutations.translateToPut(mainPut,null);
        for(byte[] indexPos:mainColPos){
            byte[] data;
            if(mainPut.has(SpliceConstants.DEFAULT_FAMILY_BYTES,indexPos))
                data = mainPut.get(SpliceConstants.DEFAULT_FAMILY_BYTES,indexPos).get(0).getValue();
            else
                data = r.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES,indexPos);

            newPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES,indexPos,data);
        }

        return newPut;
    }

    /*
     * Convenience wrapper around constructing a proper index row array.
     */
    private byte[][] getDataArray() {
        byte[][] rowKeyBuilder;
        if(isUnique)
            rowKeyBuilder = new byte[indexColsToMainColMap.length][];
        else
            rowKeyBuilder = new byte[indexColsToMainColMap.length+1][];
        return rowKeyBuilder;
    }

    /*
     * Actually perform the put. Convenience around error management.
     */
    private void doPut(Put put, CallBuffer<Mutation> writeBuffer) throws Exception {
        try {
            writeBuffer.add(put);
        } catch (IOException ioe) {
            if(!(ioe instanceof RetriesExhaustedWithDetailsException))
                throw ioe;
            RetriesExhaustedWithDetailsException rewde = (RetriesExhaustedWithDetailsException)ioe;
            SpliceLogUtils.error(LOG, rewde.getMessage(), rewde);
            /*
             * RetriesExhaustedWithDetailsException wraps out client puts that
             * fail because of an IOException on the other end of the RPC, including
             * Constraint Violations and other DoNotRetry exceptions. Thus,
             * if we find a DoNotRetryIOException somewhere, we unwrap and throw
             * that instead of throwing a normal RetriesExhausted error.
             */
            List<Throwable> errors = rewde.getCauses();
            for(Throwable t:errors){
                if(t instanceof DoNotRetryIOException)
                    throw (DoNotRetryIOException)t;
            }

            throw rewde;
        }
    }





    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexManager)) return false;

        IndexManager that = (IndexManager) o;

        return indexConglomId == that.indexConglomId;
    }

    @Override
    public int hashCode() {
        return 31 * 17 + (int) (indexConglomId ^ (indexConglomId >>> 32));
    }

    /**
     * Creates a new IndexManager from the specified IndexDescriptor
     *
     * @param indexConglomId the conglomerate of the destination index
     * @param indexDescriptor an Index descriptor for the index.
     * @throws IOException if something goes wrong allocating the underlying table pool.
     */
    public static IndexManager create(long indexConglomId,IndexDescriptor indexDescriptor) throws IOException {
        return new IndexManager(indexConglomId,indexDescriptor.baseColumnPositions(),indexDescriptor.isUnique());
    }

    public static IndexManager create(long indexConglomId,int[] indexColsToMainColMap,boolean isUnique) throws IOException {
        return new IndexManager(indexConglomId,indexColsToMainColMap,isUnique);
    }

    /**
     * Creates a read-only IndexManager, which does not allocate an underlying Table entry.
     *
     * This is primarily useful for deleting entries out of the index sets and other operational stuff
     * that doesn't affect the index directly, or for translating KeyValues into index puts but not
     * actually performing the put
     *
     * @param indexConglomId the conglomerate of the index
     * @param indexColsToMainColMap the mapping from index column entries to main column entries.
     * @param isUnique if this index is unique
     * @return a read-only IndexManager
     */
    public static IndexManager emptyTable(long indexConglomId, int[] indexColsToMainColMap, boolean isUnique) {
        return new IndexManager(indexConglomId,indexColsToMainColMap,isUnique){
            @Override
            public void update(Mutation mutation, RegionCoprocessorEnvironment rce) throws IOException {
                throw new UnsupportedOperationException("Cannot write Index with a read-only IndexManager");
            }

            @Override
            public void update(Collection<Mutation> mutations, RegionCoprocessorEnvironment rce) throws IOException {
                throw new UnsupportedOperationException("Cannot write Index with a read-only IndexManager");
            }
        };
    }

}
