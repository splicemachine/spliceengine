package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.actions.UpdateConstantOperation;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.util.BitSet;
import java.util.Collections;

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

        return new UpdateRowEncoder(finalPkColumns,null,null,null,
                KeyType.BARE,
                RowMarshaller.sparsePacked(),
                modifiedPks,finalHeapList,colPositionMap);
    }

    @Override
	public String toString() {
		return "Update{destTable="+heapConglom+",source=" + source + "}";
	}

    private class UpdateRowEncoder extends RowEncoder{

        private final boolean modifiedPks;
        private FormatableBitSet finalHeapList;
        private int[] colPositionMap;
        private byte[] filterBytes;
        private MultiFieldDecoder heapDecoder;
        private EntryEncoder nonPkEncoder;


        protected UpdateRowEncoder(int[] keyColumns,
                                   boolean[] keySortOrder,
                                   int[] rowColumns,
                                   byte[] keyPrefix,
                                   KeyMarshall keyType,
                                   RowMarshall rowType,
                                   boolean modifiedPks,
                                   FormatableBitSet finalHeapList,
                                   int[] colPositionMap) throws StandardException {
            super(keyColumns, keySortOrder, rowColumns, keyPrefix, keyType, rowType);
            this.modifiedPks = modifiedPks;
            this.finalHeapList = finalHeapList;
            this.colPositionMap = colPositionMap;

        }

        private byte[] getFilterBytes() throws StandardException {
            if(filterBytes!=null) return filterBytes;

            //we need the index so that we can transform data without the information necessary to decode it
            EntryPredicateFilter predicateFilter = new EntryPredicateFilter(new BitSet(),Collections.<Predicate>emptyList(),true);
            this.filterBytes = predicateFilter.toBytes();
            return filterBytes;
        }

        @Override
        public void write(ExecRow nextRow, CallBuffer<KVPair> buffer) throws Exception {
            RowLocation location= (RowLocation)nextRow.getColumn(nextRow.nColumns()).getObject(); //the location to update is always at the end
	        /*
	         * If we have primary keys, it's possible that we have modified one of them, which will change the row
	         * location. In that case, the location above isn't used for a Put, but for a delete instead.
	         *
	         * To find out, we take the intersection between heapList and pkColumns and, if it's non-empty, we
	         * deal with primary keys. Otherwise, we do an update as usual
	         */
            if(!modifiedPks){
//                Put put = SpliceUtils.createPut(location.getBytes(),((TransactionalCallBuffer)buffer).getTransactionId());
//                put.setAttribute(Puts.PUT_TYPE,Puts.FOR_UPDATE);

                //set the columns from the new row
                EntryEncoder encoder = getNonPkEncoder(nextRow);

                MultiFieldEncoder fieldEncoder = encoder.getEntryEncoder();
                fieldEncoder.reset();
                for(int i=finalHeapList.anySetBit();i>=0;i=finalHeapList.anySetBit(i)){
                    DataValueDescriptor dvd = nextRow.getRowArray()[colPositionMap[i]];
                    //we know that derby never spits out a null field here--we hope.
                    if(dvd.isNull())
                        DerbyBytesUtil.encodeTypedEmpty(fieldEncoder,dvd,false,true);
                    else
                        DerbyBytesUtil.encodeInto(fieldEncoder, dvd, false);
                }
                byte[] value = encoder.encode();

                buffer.add(new KVPair(location.getBytes(),value, KVPair.Type.UPDATE));
            }else{
            	Result result = null;
            	HTableInterface htable = null;
            	try {
            		htable = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(Long.toString(heapConglom)));
            		if (LOG.isTraceEnabled())
            			SpliceLogUtils.trace(LOG,"UpdateOperation sink: primary keys modified");
                    /*
                     * Since we modified a primary key, we have to delete the data and reinsert it into the new location
                     * That means we have to do a Get to get the full row, then merge it with the update values,
                     * re-insert it in the new location, and then delete the old location.
                     */
            		Get remoteGet = SpliceUtils.createGet(getTransactionID(), location.getBytes());
            		remoteGet.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY);
            		remoteGet.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,getFilterBytes());
            		result = htable.get(remoteGet);
            	}  finally {
            		Closeables.closeQuietly(htable);
            	}

                //convert Result into put under the new row key
                keyEncoder.reset();
                keyType.encodeKey(nextRow.getRowArray(),keyColumns,keySortOrder,keyPostfix,keyEncoder);

                byte[] rowKey = keyEncoder.build();


                if(heapDecoder==null)
                    heapDecoder = MultiFieldDecoder.create();

                EntryDecoder getDecoder = new EntryDecoder();
                getDecoder.set(result.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY));

                EntryEncoder entryEncoder = EntryEncoder.create(getDecoder.getCurrentIndex());

                BitIndex index = getDecoder.getCurrentIndex();
                MultiFieldEncoder updateEncoder = entryEncoder.getEntryEncoder();
                MultiFieldDecoder getFieldDecoder = getDecoder.getEntryDecoder();
                for(int pos=index.nextSetBit(0);pos>=0;pos=index.nextSetBit(pos+1)){
                    if(finalHeapList.isSet(pos+1)){
                        DataValueDescriptor dvd = nextRow.getRowArray()[colPositionMap[pos+1]];
                        if(dvd==null||dvd.isNull()){
                            updateEncoder.encodeEmpty();
                        }else{
                            DerbyBytesUtil.encodeInto(updateEncoder, dvd,false);
                        }
                        getDecoder.seekForward(getFieldDecoder,pos);
                    }else{
                        //use the index to get the correct offsets
                        int offset = getFieldDecoder.offset();
                        getDecoder.seekForward(getFieldDecoder,pos);
                        int limit = getFieldDecoder.offset()-1-offset;
                        updateEncoder.setRawBytes(getFieldDecoder.array(),offset,limit);
                    }
                }

                byte[] value = entryEncoder.encode();
                buffer.add(new KVPair(rowKey,value));
                buffer.add(new KVPair(location.getBytes(),EMPTY_BYTES, KVPair.Type.DELETE));
            }
        }

        private EntryEncoder getNonPkEncoder(ExecRow nextRow) {
            if(nonPkEncoder==null){
                BitSet setColumns = new BitSet(finalHeapList.size());
                BitSet scalarFields = new BitSet();
                BitSet floatFields = new BitSet();
                BitSet doubleFields = new BitSet();
                for(int i=finalHeapList.anySetBit();i>=0;i=finalHeapList.anySetBit(i)){
                    setColumns.set(i-1);
                    DataValueDescriptor dvd = nextRow.getRowArray()[colPositionMap[i]];
                    if(DerbyBytesUtil.isScalarType(dvd)){
                        scalarFields.set(i-1);
                    }else if(DerbyBytesUtil.isFloatType(dvd)){
                        floatFields.set(i-1);
                    }else if(DerbyBytesUtil.isDoubleType(dvd)){
                        doubleFields.set(i-1);
                    }
                }
                nonPkEncoder= EntryEncoder.create(nextRow.nColumns(),setColumns,scalarFields,floatFields,doubleFields);
            }
            return nonPkEncoder;
        }
    }
}
