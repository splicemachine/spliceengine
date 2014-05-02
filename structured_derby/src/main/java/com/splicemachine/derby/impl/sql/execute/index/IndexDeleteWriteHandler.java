package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class IndexDeleteWriteHandler extends AbstractIndexWriteHandler {

    private final List<KVPair> deletes = Lists.newArrayListWithExpectedSize(0);
    private final IndexTransformer2 transformer;
    private CallBuffer<KVPair> indexBuffer;
    private final int expectedWrites;

    public IndexDeleteWriteHandler(BitSet indexedColumns,
                                   int[] mainColToIndexPosMap,
                                   byte[] indexConglomBytes,
                                   BitSet descColumns,
                                   boolean keepState,
                                   int expectedWrites,
                                   int[] columnOrdering,
                                   int[] formatIds){
        this(indexedColumns,mainColToIndexPosMap,indexConglomBytes,descColumns,
             keepState,false,false,expectedWrites, columnOrdering, formatIds);
    }

    public IndexDeleteWriteHandler(BitSet indexedColumns,
                                   int[] keyEncodingMap,
                                   byte[] indexConglomBytes,
                                   BitSet descColumns,
                                   boolean keepState,
                                   boolean unique,
                                   boolean uniqueWithDuplicateNulls,
                                   int expectedWrites,
                                   int[] keyColumnEncodingOrder,
                                   int[] mainTableTypes){
        super(indexedColumns,keyEncodingMap,indexConglomBytes,descColumns,keepState);
        BitSet nonUniqueIndexColumn = (BitSet)translatedIndexColumns.clone();
        nonUniqueIndexColumn.set(translatedIndexColumns.length());
        this.expectedWrites = expectedWrites;
				boolean[] destAscDescColumns = new boolean[keyEncodingMap.length];
				Arrays.fill(destAscDescColumns, true);
				for(int key:keyEncodingMap){
						if(descColumns.get(key))
								destAscDescColumns[key] = false;
				}
				int[] keyDecodingMap = new int[(int)indexedColumns.length()];
				Arrays.fill(keyDecodingMap,-1);
				for(int i=indexedColumns.nextSetBit(0);i>=0; i= indexedColumns.nextSetBit(i+1)){
						keyDecodingMap[i] = keyEncodingMap[i];
				}
				this.transformer = new IndexTransformer2(
								unique,
								uniqueWithDuplicateNulls,
								null, //TODO -sf- make this table version match
								keyColumnEncodingOrder,
								mainTableTypes,
								null,
								keyDecodingMap,
								destAscDescColumns);
//        this.transformer = new IndexTransformer(indexedColumns,
//                translatedIndexColumns,nonUniqueIndexColumn,descColumns,keyEncodingMap,unique,
//                uniqueWithDuplicateNulls, SpliceDriver.getKryoPool(),keyColumnEncodingOrder,mainTableTypes);
    }

    @Override
    protected boolean updateIndex(KVPair mutation, WriteContext ctx) {
        if(mutation.getType()!= KVPair.Type.DELETE) return true;
        if(indexBuffer==null){
            try{
                indexBuffer = getWriteBuffer(ctx,expectedWrites);
            }catch(Exception e){
                ctx.failed(mutation,WriteResult.failed(e.getClass().getSimpleName()+":"+e.getMessage()));
                failed=true;
            }
        }

        delete(mutation,ctx);
        return !failed;
    }

    @Override
    protected void finish(final WriteContext ctx) throws Exception {
        if(indexBuffer!=null){
            indexBuffer.flushBuffer();
            indexBuffer.close();
        }
    }

    private void delete(KVPair mutation, WriteContext ctx) {
        /*
         * To delete the correct row, we do the following:
         *
         * 1. do a Get() on all the indexed columns of the main table
         * 2. transform the results into an index row (as if we were inserting it)
         * 3. issue a delete against the index table
         */
        try {
            Get get = SpliceUtils.createGet(ctx.getTransactionId(), mutation.getRow());
            EntryPredicateFilter predicateFilter = new EntryPredicateFilter(indexedColumns, new ObjectArrayList<Predicate>(),true);
            get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());
            Result result = ctx.getRegion().get(get);
            if(result==null||result.isEmpty()){
                //already deleted? Weird, but okay, we can deal with that
                ctx.success(mutation);
                return;
            }

            KeyValue resultValue = null;
            for(KeyValue value:result.raw()){
                if(value.matchingColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES)){
                    resultValue = value;
                    break;
                }
            }
            KVPair resultPair = new KVPair(get.getRow(),resultValue.getValue(), KVPair.Type.DELETE);
            KVPair indexDelete = transformer.translate(resultPair);
            indexBuffer.add(indexDelete);
        } catch (IOException e) {
            failed=true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName()+":"+e.getMessage()));
        } catch (Exception e) {
            e.printStackTrace();
            failed=true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName()+":"+e.getMessage()));
        }
    }

	@Override
	public void next(List<KVPair> mutations, WriteContext ctx) {
		// XXX JLEACH TODO
		throw new RuntimeException("Not Supported");
	}

}
