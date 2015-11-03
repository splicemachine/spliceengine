package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.sql.execute.actions.UpdateConstantOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.stream.function.InsertPairFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.temporary.WriteReadUtils;
import com.splicemachine.derby.stream.temporary.update.*;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.callbuffer.ForwardRecordingCallBuffer;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.TxnView;
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
import java.util.ArrayList;
import java.util.Collection;

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
    protected int[] columnOrdering;
    protected int[] format_ids;

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
                           GeneratedMethod checkGM, Activation activation,double optimizerEstimatedRowCount,
                           double optimizerEstimatedCost)
            throws StandardException, IOException {
        super(source, generationClauses, checkGM, activation,optimizerEstimatedRowCount,optimizerEstimatedCost);
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
            format_ids = conglomerate.getFormat_ids();
            columnOrdering = conglomerate.getColumnOrdering();
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
		public RecordingCallBuffer<KVPair> transformWriteBuffer(final RecordingCallBuffer<KVPair> bufferToTransform) throws StandardException {
				if(modifiedPrimaryKeys(getHeapList())){
                    TxnView txn = bufferToTransform.getTxn();
                        WriteCoordinator writeCoordinator = SpliceDriver.driver().getTableWriter();
                        return new ForwardRecordingCallBuffer<KVPair>(writeCoordinator.writeBuffer(getDestinationTable(),txn, new PreFlushHook() {
                            @Override
                            public Collection<KVPair> transform(Collection<KVPair> buffer) throws Exception {
                                bufferToTransform.flushBufferAndWait();
                                return new ArrayList<KVPair>(buffer); // Remove this but here to match no op...
                            }
                        })){
								@Override
								public void add(KVPair element) throws Exception {
										byte[] oldLocation = ((RowLocation) currentRow.getColumn(currentRow.nColumns()).getObject()).getBytes();
                                        if (!Bytes.equals(oldLocation, element.getRowKey())) {
                                            bufferToTransform.add(new KVPair(oldLocation, HConstants.EMPTY_BYTE_ARRAY, KVPair.Type.DELETE));
                                            element.setType(KVPair.Type.INSERT);
                                        } else {
                                            element.setType(KVPair.Type.UPDATE);
                                        }
                                    delegate.add(element);
								}
						};
				} else
                    return bufferToTransform;
		}

		@Override
		public String toString() {
				return "Update{destTable="+heapConglom+",source=" + source + "}";
		}

    @Override
    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        DataSet set = source.getDataSet(dsp);
        OperationContext operationContext = dsp.createOperationContext(this);
        TxnView txn = getCurrentTransaction();
        ExecRow execRow = getExecRowDefinition();
        int[] execRowTypeFormatIds = WriteReadUtils.getExecRowTypeFormatIds(execRow);
        UpdateTableWriterBuilder builder = new UpdateTableWriterBuilder()
                .heapConglom(heapConglom)
                .operationContext(operationContext)
                .execRowDefinition(execRow)
                .execRowTypeFormatIds(execRowTypeFormatIds)
                .pkCols(pkCols==null?new int[0]:pkCols)
                .pkColumns(pkColumns)
                .formatIds(format_ids)
                .columnOrdering(columnOrdering==null?new int[0]:columnOrdering)
                .heapList(getHeapList())
                .tableVersion(writeInfo.getTableVersion())
                .txn(txn);
        try {
            operationContext.pushScope("Update");
            return set.index(new InsertPairFunction(operationContext)).updateData(builder, operationContext);
        } finally {
            operationContext.popScope();
        }
    }
}
