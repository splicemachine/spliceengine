package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.sql.execute.actions.UpdateConstantOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.stream.temporary.update.NonPkRowHash;
import com.splicemachine.derby.stream.temporary.update.PkDataHash;
import com.splicemachine.derby.stream.temporary.update.PkRowHash;
import com.splicemachine.derby.stream.temporary.update.ResultSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.callbuffer.ForwardRecordingCallBuffer;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * @author jessiezhang
 * @author Scott Fines
 */
public class UpdateOperation extends DMLWriteOperation{
    private static final Logger LOG = Logger.getLogger(UpdateOperation.class);
    private ResultSupplier resultSupplier;
    private DataValueDescriptor[] kdvds;
    public int[] colPositionMap;
    public FormatableBitSet heapList;

    protected static final String NAME = UpdateOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}
	
    @SuppressWarnings("UnusedDeclaration")
		public UpdateOperation() {
				super();
		}

    int[] pkCols;
    FormatableBitSet     pkColumns;

    public UpdateOperation(SpliceOperation source, GeneratedMethod generationClauses,
                           GeneratedMethod checkGM, Activation activation)
            throws StandardException, IOException {
        super(source, generationClauses, checkGM, activation);
        init(SpliceOperationContext.newContext(activation));
        recordConstructorTime();
    }

    @Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				SpliceLogUtils.trace(LOG,"init");
				super.init(context);
				heapConglom = writeInfo.getConglomerateId();

				pkCols = writeInfo.getPkColumnMap();
            pkColumns = writeInfo.getPkColumns();

            SpliceConglomerate conglomerate = (SpliceConglomerate)((SpliceTransactionManager)activation.getTransactionController()).findConglomerate(heapConglom);
            int[] format_ids = conglomerate.getFormat_ids();
            int[] columnOrdering = conglomerate.getColumnOrdering();
            kdvds = new DataValueDescriptor[columnOrdering.length];
            for (int i = 0; i < columnOrdering.length; ++i) {
                kdvds[i] = LazyDataValueFactory.getLazyNull(format_ids[columnOrdering[i]]);
            }

		}

    public int[] getColumnPositionMap(FormatableBitSet heapList) {
        if(colPositionMap==null) {
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
            colPositionMap = new int[heapList.size()];

            for(int i = heapList.anySetBit(),pos=heapList.getNumBitsSet();i!=-1;i=heapList.anySetBit(i),pos++){
                colPositionMap[i] = pos;
            }
        }
        return colPositionMap;
    }

		@Override
		public KeyEncoder getKeyEncoder() throws StandardException {
				DataHash hash;
				FormatableBitSet heapList = getHeapList();
				if(!modifiedPrimaryKeys(heapList)){
						hash = new DataHash<ExecRow>() {
								private ExecRow currentRow;
								@Override
								public void setRow(ExecRow rowToEncode) {
										this.currentRow = rowToEncode;
								}

								@Override
								public byte[] encode() throws StandardException, IOException {
										return ((RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject()).getBytes();
								}

								@Override public void close() throws IOException {  }

								@Override
								public KeyHashDecoder getDecoder() {
										return NoOpKeyHashDecoder.INSTANCE;
								}
						};
				}else{
						//TODO -sf- we need a sort order here for descending columns, don't we?
						//hash = BareKeyHash.encoder(getFinalPkColumns(getColumnPositionMap(heapList)),null);
                    hash = new PkDataHash(getFinalPkColumns(getColumnPositionMap(heapList)), kdvds,writeInfo.getTableVersion());
				}
				return new KeyEncoder(NoOpPrefix.INSTANCE,hash,NoOpPostfix.INSTANCE);
		}

		@Override
		public DataHash getRowHash() throws StandardException {
				FormatableBitSet heapList = getHeapList();
				boolean modifiedPrimaryKeys = modifiedPrimaryKeys(heapList);


				final int[] colPositionMap = getColumnPositionMap(heapList);

				//if we haven't modified any of our primary keys, then we can just change it directly
				DescriptorSerializer[] serializers = VersionedSerializers.forVersion(writeInfo.getTableVersion(),false).getSerializers(getExecRowDefinition());
				if(!modifiedPrimaryKeys){
						return new NonPkRowHash(colPositionMap,null, serializers, heapList);
				}

				int[] finalPkColumns = getFinalPkColumns(colPositionMap);

				resultSupplier = new ResultSupplier(new BitSet(),txn,heapConglom);
				return new PkRowHash(finalPkColumns,null,heapList,colPositionMap,resultSupplier,serializers);
		}

		private int[] getFinalPkColumns(int[] colPositionMap) {
				int[] finalPkColumns;
				if(pkCols!=null){
						finalPkColumns =new int[pkCols.length];
						int count = 0;
						for(int i : pkCols){
								finalPkColumns[count] = colPositionMap[i];
								count++;
						}
				}else{
						finalPkColumns = new int[0];
				}
				return finalPkColumns;
		}

    public FormatableBitSet getHeapList() throws StandardException{
        if(heapList==null){
            heapList = ((UpdateConstantOperation)writeInfo.getConstantAction()).getBaseRowReadList();
            if(heapList==null){
                ExecRow row = ((UpdateConstantOperation)writeInfo.getConstantAction()).getEmptyHeapRow(activation.getLanguageConnectionContext());
                int length = row.getRowArray().length;
                heapList = new FormatableBitSet(length+1);

                for(int i = 1; i < length+1; ++i){
                    heapList.set(i);
                }
            }
        }
				return heapList;
		}

		private boolean modifiedPrimaryKeys(FormatableBitSet heapList) {
				boolean modifiedPrimaryKeys = false;
				if(pkColumns!=null){
						for(int pkCol = pkColumns.anySetBit();pkCol!=-1;pkCol= pkColumns.anySetBit(pkCol)){
								if(heapList.isSet(pkCol+1)){
										modifiedPrimaryKeys = true;
										break;
								}
						}
				}
				return modifiedPrimaryKeys;
		}


		@Override
		public RecordingCallBuffer<KVPair> transformWriteBuffer(RecordingCallBuffer<KVPair> bufferToTransform) throws StandardException {
				if(modifiedPrimaryKeys(getHeapList())){
						return new ForwardRecordingCallBuffer<KVPair>(bufferToTransform){
								@Override
								public void add(KVPair element) throws Exception {
										byte[] oldLocation = ((RowLocation) currentRow.getColumn(currentRow.nColumns()).getObject()).getBytes();
                                        if (!Bytes.equals(oldLocation, element.getRowKey())) {
                                            // only add the delete if we aren't overwriting the same row
										    delegate.add(new KVPair(oldLocation, HConstants.EMPTY_BYTE_ARRAY, KVPair.Type.DELETE));
                                        }
										delegate.add(element);
								}
						};
				} else return bufferToTransform;
		}

		@Override
		public String toString() {
				return "Update{destTable="+heapConglom+",source=" + source + "}";
		}

}
