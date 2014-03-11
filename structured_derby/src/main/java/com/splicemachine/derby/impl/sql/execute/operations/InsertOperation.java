package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.actions.InsertConstantOperation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequenceKey;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.HasIncrement;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;
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
		private HTableInterface sysColumnTable;
		private int[] pkCols;
		protected boolean autoincrementGenerated;
		private boolean	singleRowResultSet = false;
		private long nextIncrement = -1;
		private RowLocation[] autoIncrementRowLocationArray;
		

		@SuppressWarnings("UnusedDeclaration")
		public InsertOperation(){ super(); }

		public InsertOperation(SpliceOperation source,
													 GeneratedMethod generationClauses,
													 GeneratedMethod checkGM) throws StandardException{
				super(source, generationClauses, checkGM, source.getActivation());
				recordConstructorTime();
		}

		public InsertOperation(SpliceOperation source,
													 OperationInformation opInfo,
													 DMLWriteInfo writeInfo,
													 String txnId) throws StandardException {
				super(source, opInfo, writeInfo);
				transactionID = txnId;
				autoIncrementRowLocationArray = writeInfo.getConstantAction() != null &&
						((InsertConstantOperation) writeInfo.getConstantAction()).getAutoincRowLocation() != null?
								((InsertConstantOperation) writeInfo.getConstantAction()).getAutoincRowLocation():new RowLocation[0];
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException{
				super.init(context);
				heapConglom = writeInfo.getConglomerateId();
				pkCols = writeInfo.getPkColumnMap();
				singleRowResultSet = isSingleRowResultSet();
				autoIncrementRowLocationArray = writeInfo.getConstantAction() != null &&
						((InsertConstantOperation) writeInfo.getConstantAction()).getAutoincRowLocation() != null?
								((InsertConstantOperation) writeInfo.getConstantAction()).getAutoincRowLocation():new RowLocation[0];

		}

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext){
				HashPrefix prefix;
				DataHash dataHash;
				KeyPostfix postfix = NoOpPostfix.INSTANCE;
				if(pkCols==null){
						prefix = new SaltedPrefix(operationInformation.getUUIDGenerator());
						dataHash = NoOpDataHash.INSTANCE;
				}else{
						int[] keyColumns = new int[pkCols.length];
						for(int i=0;i<keyColumns.length;i++){
								keyColumns[i] = pkCols[i] -1;
						}
						prefix = NoOpPrefix.INSTANCE;
						dataHash = BareKeyHash.encoder(keyColumns,null, spliceRuntimeContext.getKryoPool());
				}

				return new KeyEncoder(prefix,dataHash,postfix);
		}

        private int[] getEncodingColumns(int n) {
            int[] columns = IntArrays.count(n);

            // Skip primary key columns to save space
            if (pkCols != null) {
                for(int pkCol:pkCols) {
                    columns[pkCol-1] = -1;
                }
            }
            return columns;
        }
		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				//get all columns that are being set
				ExecRow defnRow = getExecRowDefinition();
				int[] columns = getEncodingColumns(defnRow.nColumns());
				return new EntryDataHash(columns,null);
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
				int index = columnPosition-1;

				HBaseRowLocation rl = (HBaseRowLocation) autoIncrementRowLocationArray[index];

				byte[] rlBytes = rl.getBytes();

				if(sysColumnTable==null){
						sysColumnTable = SpliceAccessManager.getHTable(SpliceConstants.SEQUENCE_TABLE_NAME_BYTES);
				}

				SpliceSequence sequence;
				try {
					if (singleRowResultSet) { // Single Sequence Move
						sequence = SpliceDriver.driver().getSequencePool().get(new SpliceSequenceKey(sysColumnTable,rlBytes,
								heapConglom,columnPosition,activation.getLanguageConnectionContext().getDataDictionary(),1l));						
						nextIncrement = sequence.getNext();
						this.getActivation().getLanguageConnectionContext().setIdentityValue(nextIncrement);
					} else {
						sequence = SpliceDriver.driver().getSequencePool().get(new SpliceSequenceKey(sysColumnTable,rlBytes,
								heapConglom,columnPosition,activation.getLanguageConnectionContext().getDataDictionary(),SpliceConstants.sequenceBlockSize));						
						nextIncrement = sequence.getNext();						
					}
				} catch (Exception e) {
						throw Exceptions.parseException(e);
				}				

				if(rowTemplate==null)
						rowTemplate = getExecRowDefinition();
				DataValueDescriptor dvd = rowTemplate.cloneColumn(columnPosition);
				dvd.setValue(nextIncrement);
				return dvd;
		}

		@Override
		public void close() throws StandardException, IOException {
				super.close();
				if(sysColumnTable!=null){
						try{
								sysColumnTable.close();
						} catch (IOException e) {
								SpliceLogUtils.error(LOG,"Unable to close htable, beware of potential memory problems!",e);
						}
				}
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
			init(SpliceOperationContext.newContext(activation));
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
		
		
	    
	    
}
