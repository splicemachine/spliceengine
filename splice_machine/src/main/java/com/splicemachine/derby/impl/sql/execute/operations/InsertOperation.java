package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.impl.sql.compile.InsertNode;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.actions.InsertConstantOperation;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.DMLWriteInfo;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.OperationInformation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceIdentityColumnKey;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stream.function.InsertPairFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.temporary.WriteReadUtils;
import com.splicemachine.derby.stream.temporary.insert.InsertTableWriter;
import com.splicemachine.derby.stream.temporary.insert.InsertTableWriterBuilder;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.HasIncrement;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 *
 * Operation that handles inserts into Splice Machine.
 *
 * @author Scott Fines
 *
 *
 */
public class InsertOperation extends DMLWriteOperation implements HasIncrement {
		private static final long serialVersionUID = 1l;
		private static final Logger LOG = Logger.getLogger(InsertOperation.class);
		private ExecRow rowTemplate;
		private int[] pkCols;
		private boolean	singleRowResultSet = false;
		private long nextIncrement = -1;
		private RowLocation[] autoIncrementRowLocationArray;
        private SpliceSequence[] spliceSequences;
	    protected static final String NAME = InsertOperation.class.getSimpleName().replaceAll("Operation","");
        public InsertTableWriter tableWriter;
        public Pair<Long,Long>[] defaultAutoIncrementValues;
        public InsertNode.InsertMode insertMode;
        public String statusDirectory;
        public int failBadRecordCount;


    @Override
		public String getName() {
				return NAME;
		}

		@SuppressWarnings("UnusedDeclaration")
		public InsertOperation(){ super(); }

		public InsertOperation(SpliceOperation source,
													 GeneratedMethod generationClauses,
													 GeneratedMethod checkGM,String insertMode, String statusDirectory, int failBadRecordCount,double optimizerEstimatedRowCount,
                                                     double optimizerEstimatedCost) throws StandardException{
				super(source, generationClauses, checkGM, source.getActivation(),optimizerEstimatedRowCount,optimizerEstimatedCost);
                this.insertMode = InsertNode.InsertMode.valueOf(insertMode);
                this.statusDirectory = statusDirectory;
                this.failBadRecordCount = failBadRecordCount;
            try {
                init(SpliceOperationContext.newContext(activation));
            } catch (IOException ioe) {
                Exceptions.parseException(ioe);
            }
				recordConstructorTime();
		}

/*		public InsertOperation(SpliceOperation source,
                           OperationInformation opInfo,
                           DMLWriteInfo writeInfo) throws StandardException {
				super(source, opInfo, writeInfo);
                try {
                    init(SpliceOperationContext.newContext(activation));
                } catch (IOException ioe) {
                    Exceptions.parseException(ioe);
                }
		}
*/
		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
            try {
                super.init(context);
                writeInfo.initialize(context);
                heapConglom = writeInfo.getConglomerateId();
                pkCols = writeInfo.getPkColumnMap();
                singleRowResultSet = isSingleRowResultSet();
                autoIncrementRowLocationArray = writeInfo.getConstantAction() != null &&
                        ((InsertConstantOperation) writeInfo.getConstantAction()).getAutoincRowLocation() != null ?
                        ((InsertConstantOperation) writeInfo.getConstantAction()).getAutoincRowLocation() : new RowLocation[0];
                defaultAutoIncrementValues = WriteReadUtils.getStartAndIncrementFromSystemTables(autoIncrementRowLocationArray,
                        activation.getLanguageConnectionContext().getDataDictionary(),
                        heapConglom);
                spliceSequences = new SpliceSequence[autoIncrementRowLocationArray.length];
                int length = autoIncrementRowLocationArray.length;
                for (int i = 0; i < length; i++) {
                    HBaseRowLocation rl = (HBaseRowLocation) autoIncrementRowLocationArray[i];
                    if (rl == null) {
                        spliceSequences[i] = null;
                    } else {
                        byte[] rlBytes = rl.getBytes();
                        spliceSequences[i] = SpliceDriver.driver().getSequencePool().get(new SpliceIdentityColumnKey(
                                rlBytes,
                                (isSingleRowResultSet()) ? 1l : SpliceConstants.sequenceBlockSize,
                                defaultAutoIncrementValues[i].getFirst(),
                                defaultAutoIncrementValues[i].getSecond()));
                    }
                }
            } catch (Exception e) {
                throw Exceptions.parseException(e);
            }

        }

		@Override
		public String toString() {
				return "Insert{destTable="+heapConglom+",source=" + source + "}";
		}

		@Override
		public String prettyPrint(int indentLevel) {
				return "Insert"+super.prettyPrint(indentLevel);
		}

		@Override
		public DataValueDescriptor increment(int columnPosition, long increment) throws StandardException {
            nextIncrement = ((BaseActivation) activation).ignoreSequence()?-1:spliceSequences[columnPosition - 1].getNext();
            this.getActivation().getLanguageConnectionContext().setIdentityValue(nextIncrement);
            if (rowTemplate==null)
                rowTemplate = getExecRowDefinition();
            DataValueDescriptor dvd = rowTemplate.cloneColumn(columnPosition);
            dvd.setValue(nextIncrement);
            return dvd;
		}

		@Override
		public void close() throws StandardException {
				super.close();
				if (nextIncrement != -1) // Do we do this twice?
					this.getActivation().getLanguageConnectionContext().setIdentityValue(nextIncrement);					
		}
				
	    private boolean isSingleRowResultSet()
	    {
	        boolean isRow = false;
	        if (source instanceof RowOperation)
	        	isRow = true;
	        else if (source instanceof NormalizeOperation)
	            isRow = (((NormalizeOperation) source).source instanceof RowOperation);
	        return isRow;
	    }

		@Override
		public void setActivation(Activation activation) throws StandardException {
			super.setActivation(activation);

				SpliceOperationContext context = SpliceOperationContext.newContext(activation);
				writeInfo.initialize(context);
				heapConglom = writeInfo.getConglomerateId();
				pkCols = writeInfo.getPkColumnMap();
				singleRowResultSet = isSingleRowResultSet();
				autoIncrementRowLocationArray = writeInfo.getConstantAction() != null &&
								((InsertConstantOperation) writeInfo.getConstantAction()).getAutoincRowLocation() != null?
								((InsertConstantOperation) writeInfo.getConstantAction()).getAutoincRowLocation():new RowLocation[0];
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
			super.readExternal(in);
			autoIncrementRowLocationArray = new RowLocation[in.readInt()];
			for (int i = 0; i < autoIncrementRowLocationArray.length; i++) {
				autoIncrementRowLocationArray[i] = (HBaseRowLocation) in.readObject(); 
			}
            insertMode = InsertNode.InsertMode.valueOf(in.readUTF());
            if (in.readBoolean())
                statusDirectory = in.readUTF();
            failBadRecordCount = in.readInt();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
			int length = autoIncrementRowLocationArray.length;
			out.writeInt(length);
			for (int i = 0; i < length; i++) {
				out.writeObject(autoIncrementRowLocationArray[i]);
			}
            out.writeUTF(insertMode.toString());
            out.writeBoolean(statusDirectory!=null);
            if (statusDirectory!=null)
                out.writeUTF(statusDirectory);
            out.writeInt(failBadRecordCount);
		}

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        DataSet set = source.getDataSet(dsp);
        OperationContext operationContext = dsp.createOperationContext(this);
        TxnView txn = getCurrentTransaction();
        ExecRow execRow = getExecRowDefinition();
        int[] execRowTypeFormatIds = WriteReadUtils.getExecRowTypeFormatIds(execRow);
        InsertTableWriterBuilder builder = new InsertTableWriterBuilder()
                .heapConglom(heapConglom)
                .insertOperation(this)
                .autoIncrementRowLocationArray(autoIncrementRowLocationArray)
                .execRowDefinition(getExecRowDefinition())
                .execRowTypeFormatIds(execRowTypeFormatIds)
                .spliceSequences(spliceSequences)
                .pkCols(pkCols)
                .tableVersion(writeInfo.getTableVersion())
                .txn(txn);
        try {
            operationContext.pushScope("Insert");
            return set.index(new InsertPairFunction(operationContext)).insertData(builder, operationContext);
        } finally {
            operationContext.popScope();
        }
    }
}
