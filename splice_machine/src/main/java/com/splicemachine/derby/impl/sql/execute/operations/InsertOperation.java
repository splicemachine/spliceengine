package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.actions.InsertConstantOperation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.temporary.WriteReadUtils;
import com.splicemachine.derby.stream.temporary.insert.InsertTableWriter;
import com.splicemachine.derby.stream.temporary.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.utils.StreamLogUtils;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.HasIncrement;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.TxnView;
import org.apache.log4j.Logger;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 *
 * @author Scott Fines
 *
 * TODO:
 * 	1. Basic Inserts (insert 1 row, insert multiple small rows) - Done SF
 *  2. Insert with subselect (e.g. insert into t (name) select name from a) - Done SF
 *  3. Triggers (do with Coprocessors)
 *  4. Primary Keys (do with Coprocessors)
 *  5. Secondary Indices (do with Coprocessors)
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


    @Override
		public String getName() {
				return NAME;
		}

		@SuppressWarnings("UnusedDeclaration")
		public InsertOperation(){ super(); }

		public InsertOperation(SpliceOperation source,
													 GeneratedMethod generationClauses,
													 GeneratedMethod checkGM) throws StandardException{
				super(source, generationClauses, checkGM, source.getActivation());
            try {
                init(SpliceOperationContext.newContext(activation));
            } catch (IOException ioe) {
                Exceptions.parseException(ioe);
            }
				recordConstructorTime();
		}

		public InsertOperation(SpliceOperation source,
                           OperationInformation opInfo,
                           DMLWriteInfo writeInfo) throws StandardException {
				super(source, opInfo, writeInfo);
                try {
                    init(SpliceOperationContext.newContext(activation));
                } catch (IOException ioe) {
                    Exceptions.parseException(ioe);
                }
		}

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
            if (tableWriter!=null)
               return tableWriter.increment(columnPosition,increment);
            else
                throw new RuntimeException("Not Implemented");
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
			
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
			int length = autoIncrementRowLocationArray.length;
			out.writeInt(length);
			for (int i = 0; i < length; i++) {
				out.writeObject(autoIncrementRowLocationArray[i]);
			}
		}

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        DataSet set = source.getDataSet();
        TxnView txn = getCurrentTransaction();
        InsertTableWriterBuilder builder = new InsertTableWriterBuilder()
                .heapConglom(heapConglom)
                .autoIncrementRowLocationArray(autoIncrementRowLocationArray)
                .execRowDefinition(getExecRowDefinition())
                .defaultAutoIncrementValues(
                        WriteReadUtils.getStartAndIncrementFromSystemTables(autoIncrementRowLocationArray,
                                activation.getLanguageConnectionContext().getDataDictionary(),
                                heapConglom))
                .pkCols(pkCols)
                .tableVersion(writeInfo.getTableVersion())
                .txn(txn);
        return set.index(new SplicePairFunction<SpliceOperation,LocatedRow,RowLocation,ExecRow>(dsp.createOperationContext(this)) {
            int counter = 0;
            @Override
            public Tuple2<RowLocation, ExecRow> call(LocatedRow locatedRow) throws Exception {
                return new Tuple2<RowLocation, ExecRow>(locatedRow.getRowLocation(),locatedRow.getRow());
            }

            @Override
            public RowLocation genKey(LocatedRow locatedRow) {
                counter++;
                RowLocation rowLocation = locatedRow.getRowLocation();
               return rowLocation==null?new HBaseRowLocation(Bytes.toBytes(counter)):(HBaseRowLocation) rowLocation.cloneValue(true);
            }

            @Override
            public ExecRow genValue(LocatedRow locatedRow) {
                StreamLogUtils.logOperationRecordWithMessage(locatedRow,operationContext,"indexed for insert");
                return locatedRow.getRow();
            }

        }).insertData(builder);
    }
}
