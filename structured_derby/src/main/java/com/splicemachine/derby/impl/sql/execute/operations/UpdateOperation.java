package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.sql.execute.actions.UpdateConstantOperation;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.*;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.derby.utils.marshall.RowType;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;

/**
 * @author jessiezhang
 * @author Scott Fines
 */
public class UpdateOperation extends DMLWriteOperation{
	private static final Logger LOG = Logger.getLogger(UpdateOperation.class);

	public UpdateOperation() {
		super();
	}

	public UpdateOperation(NoPutResultSet source, GeneratedMethod generationClauses,
												 GeneratedMethod checkGM, Activation activation)
			throws StandardException {
		super(source, generationClauses, checkGM, activation);
		init(SpliceOperationContext.newContext(activation));
		recordConstructorTime(); 
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG,"init with regionScanner %s",regionScanner);
		super.init(context);
        UpdateConstantOperation constantAction = (UpdateConstantOperation)constants;
		heapConglom = constantAction.getConglomerateId();
        pkCols = constantAction.getPkColumns();
        if(pkCols!=null)
            pkColumns = fromIntArray(pkCols);

	}

    @Override
    public RowEncoder getRowEncoder() throws StandardException {
        boolean modifiedPrimaryKeys = false;
        FormatableBitSet heapList = ((UpdateConstantOperation)constants).getBaseRowReadList();
        if(pkColumns!=null){
            for(int pkCol = pkColumns.anySetBit();pkCol!=-1;pkCol= pkColumns.anySetBit(pkCol)){
                if(heapList.isSet(pkCol+1)){
                    modifiedPrimaryKeys = true;
                    break;
                }
            }
        }
        final boolean modifiedPks = modifiedPrimaryKeys;

        /*
	     * heapList is the position of the columns in the original row (e.g. if cols 2 and 3 are being modified,
		 * then heapList = {2,3}). We have to take that position and convert it into the actual positions
		 * in nextRow.
		 *
		 * nextRow looks like {old,old,...,old,new,new,...,new,rowLocation}, so suppose that we have
		 * heapList = {2,3}. Then nextRow = {old2,old3,new2,new3,rowLocation}. Which makes our colPositionMap
		 * look like
		 *
		 * colPositionMap[2] = 2;
		 * colPositionMap[3] = 3;
		 *
		 * But if heapList = {2}, then nextRow looks like {old2,new2,rowLocation}, which makes our colPositionMap
		 * look like
		 *
		 * colPositionMap[2] = 1
		 *
		 * in general, then
		 *
		 * colPositionMap[i= heapList.anySetBit()] = nextRow[heapList.numSetBits()]
		 * colPositionMap[heapList.anySetBit(i)] = nextRow[heapList.numSetBits()+1]
		 * ...
		 *
		 * and so forth
		 */
        if(heapList==null){
            int[] changedCols = ((UpdateConstantOperation)constants).getChangedColumnIds();
            heapList = new FormatableBitSet(changedCols.length);
            for(int colPosition:changedCols){
                heapList.grow(colPosition+1);
                heapList.set(colPosition);
            }
        }
        final int[] colPositionMap = new int[heapList.size()];
        for(int i = heapList.anySetBit(),pos=heapList.getNumBitsSet();i!=-1;i=heapList.anySetBit(i),pos++){
            colPositionMap[i] = pos;
        }

        int[] finalPkColumns;
        if(pkCols!=null){
            finalPkColumns =new int[pkCols.length];
            for(int i= pkColumns.anySetBit();i!=-1;i=pkColumns.anySetBit(i)){
                finalPkColumns[i] = colPositionMap[i+1];
            }
        }else{
            finalPkColumns = new int[0];
        }
        final FormatableBitSet finalHeapList = heapList;

        return new UpdateRowEncoder(finalPkColumns,null,null,null, KeyType.BARE,RowType.COLUMNAR,modifiedPks,finalHeapList,colPositionMap);
    }

    @Override
	public String toString() {
		return "Update{destTable="+heapConglom+",source=" + source + "}";
	}

    private class UpdateRowEncoder extends RowEncoder{

        private final boolean modifiedPks;
        private FormatableBitSet finalHeapList;
        private int[] colPositionMap;
        private HTableInterface htable;

        protected UpdateRowEncoder(int[] keyColumns,
                                   boolean[] keySortOrder,
                                   int[] rowColumns,
                                   byte[] keyPrefix,
                                   KeyMarshall keyType,
                                   RowType rowType,
                                   boolean modifiedPks,
                                   FormatableBitSet finalHeapList,
                                   int[] colPositionMap) {
            super(keyColumns, keySortOrder, rowColumns, keyPrefix, keyType, rowType);
            this.modifiedPks = modifiedPks;
            this.finalHeapList = finalHeapList;
            this.colPositionMap = colPositionMap;
        }

        @Override
        public void write(ExecRow nextRow, String txnId, CallBuffer<Mutation> buffer) throws Exception {
            RowLocation location= (RowLocation)nextRow.getColumn(nextRow.nColumns()).getObject(); //the location to update is always at the end
	        /*
	         * If we have primary keys, it's possible that we have modified one of them, which will change the row
	         * location. In that case, the location above isn't used for a Put, but for a delete instead.
	         *
	         * To find out, we take the intersection between heapList and pkColumns and, if it's non-empty, we
	         * deal with primary keys. Otherwise, we do an update as usual
	         */
            if(!modifiedPks){
                Put put = Puts.buildUpdate(location, nextRow.getRowArray(), finalHeapList,
                        colPositionMap, UpdateOperation.this.getTransactionID(),Serializer.get());
                SpliceUtils.attachTransaction(put,txnId);
                put.setAttribute(Puts.PUT_TYPE, Puts.FOR_UPDATE);
                buffer.add(put);
            }else{
                if(htable ==null){
                    htable = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(Long.toString(heapConglom)));
                }
                SpliceLogUtils.trace(LOG,"UpdateOperation sink: primary keys modified");

                    /*
                     * Since we modified a primary key, we have to delete the data and reinsert it into the new location
                     * That means we have to do a Get to get the full row, then merge it with the update values,
                     * re-insert it in the new location, and then delete the old location.
                     */
                Get remoteGet = SpliceUtils.createGet(getTransactionID(), location.getBytes());
                remoteGet.addFamily(SpliceConstants.DEFAULT_FAMILY_BYTES);
                Result result = htable.get(remoteGet);

                //convert Result into put under the new row key
                keyEncoder.reset();
                keyType.encodeKey(nextRow.getRowArray(),keyColumns,keySortOrder,keyPostfix,keyEncoder);
                Put put = new Put(keyEncoder.build());
                SpliceUtils.attachTransaction(put,txnId);
                NavigableMap<byte[],byte[]> familyMap = result.getFamilyMap(SpliceConstants.DEFAULT_FAMILY_BYTES);
                for(byte[] qualifier:familyMap.keySet()){
                    int position = Bytes.toInt(qualifier);
                    if(finalHeapList.isSet(position + 1)){
                        //put the new value into the position instead of the old one
                        DataValueDescriptor dvd = nextRow.getRowArray()[colPositionMap[position+1]];
                        put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, qualifier, DerbyBytesUtil.generateBytes(dvd));
                    }else
                        put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,qualifier,familyMap.get(qualifier));
                }

                buffer.add(put);
                buffer.add(Mutations.getDeleteOp(txnId,location.getBytes()));
            }
        }
    }
}
