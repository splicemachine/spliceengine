package com.splicemachine.derby.impl.sql.execute.operations;


import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericPreparedStatement;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IndexRowToBaseRowOperation extends SpliceBaseOperation implements CursorResultSet{

	private static Logger LOG = Logger.getLogger(IndexRowToBaseRowOperation.class);
	protected int lockMode;
	protected int isolationLevel;
//	protected ExecRow candidate;
	protected FormatableBitSet accessedCols;
	protected String resultRowAllocatorMethodName;
	protected StaticCompiledOpenConglomInfo scoci;
	protected DynamicCompiledOpenConglomInfo dcoci;
	protected SpliceOperation source;
	protected String indexName;
	protected boolean forUpdate;
	protected GeneratedMethod restriction;
	protected String restrictionMethodName;
	protected FormatableBitSet accessedHeapCols;
	protected FormatableBitSet heapOnlyCols;
	protected FormatableBitSet accessedAllCols;
	protected int[] indexCols;
	protected ExecRow resultRow;
	protected DataValueDescriptor[]	rowArray;
	protected int scociItem;
	protected long conglomId;
	protected int heapColRefItem;
	protected int allColRefItem;
	protected int heapOnlyColRefItem;
	protected int indexColMapItem;
	private ExecRow compactRow;
	RowLocation baseRowLocation = null;
//	FormatableBitSet accessFromTableCols;
	boolean copiedFromSource = false;

	/*
 	 * Variable here to stash pre-generated DataValue definitions for use in
 	 * getExecRowDefinition(). Save a little bit of performance by caching it
 	 * once created.
 	 */
//	private ExecRow definition;
	private HTableInterface table;


	public IndexRowToBaseRowOperation () {
		super();
	}

	public IndexRowToBaseRowOperation(long conglomId, int scociItem,
			Activation activation, NoPutResultSet source,
			GeneratedMethod resultRowAllocator, int resultSetNumber,
			String indexName, int heapColRefItem, int allColRefItem,
			int heapOnlyColRefItem, int indexColMapItem,
			GeneratedMethod restriction, boolean forUpdate,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost) throws StandardException {
		super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
		SpliceLogUtils.trace(LOG,"instantiate with parameters");
		this.resultRowAllocatorMethodName = resultRowAllocator.getMethodName();
        this.source = (SpliceOperation) source;
		this.indexName = indexName;
		this.forUpdate = forUpdate;
		this.scociItem = scociItem;
		this.conglomId = conglomId;
		this.heapColRefItem = heapColRefItem;
		this.allColRefItem = allColRefItem;
		this.heapOnlyColRefItem = heapOnlyColRefItem;
		this.indexColMapItem = indexColMapItem;
		this.restrictionMethodName = restriction==null? null: restriction.getMethodName();
        init(SpliceOperationContext.newContext(activation));
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		SpliceLogUtils.trace(LOG,"readExternal");
		super.readExternal(in);
		scociItem = in.readInt();
		conglomId = in.readLong();
		heapColRefItem = in.readInt();
		allColRefItem = in.readInt();
		heapOnlyColRefItem = in.readInt();
		indexColMapItem = in.readInt();		
		source = (SpliceOperation) in.readObject();
		accessedCols = (FormatableBitSet) in.readObject();
		resultRowAllocatorMethodName = in.readUTF();
		indexName = in.readUTF();
		restrictionMethodName = readNullableString(in);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG,"writeExternal");
		super.writeExternal(out);
		out.writeInt(scociItem);
		out.writeLong(conglomId);
		out.writeInt(heapColRefItem);
		out.writeInt(allColRefItem);
		out.writeInt(heapOnlyColRefItem);
		out.writeInt(indexColMapItem);
		out.writeObject(source);
		out.writeObject(accessedCols);
		out.writeUTF(resultRowAllocatorMethodName);
		out.writeUTF(indexName);
		writeNullableString(restrictionMethodName, out);
	}

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"init called");
		super.init(context);
		source.init(context);
		try {
            GenericStorablePreparedStatement statement = context.getPreparedStatement();
			if(restrictionMethodName !=null){
                SpliceLogUtils.trace(LOG,"%s:restrictionMethodName=%s",indexName,restrictionMethodName);
				restriction = statement.getActivationClass().getMethod(restrictionMethodName);
            }
			GeneratedMethod generatedMethod = statement.getActivationClass().getMethod(resultRowAllocatorMethodName);
			final GenericPreparedStatement gp = (GenericPreparedStatement)activation.getPreparedStatement();
			final Object[] saved = gp.getSavedObjects();
			scoci = (StaticCompiledOpenConglomInfo)saved[scociItem];
			TransactionController tc = activation.getTransactionController();
			dcoci = tc.getDynamicCompiledConglomInfo(conglomId);
			
            table = SpliceAccessManager.getHTable(conglomId);
			// the saved objects, if it exists
			if (heapColRefItem != -1) {
				this.accessedHeapCols = (FormatableBitSet)saved[heapColRefItem];
			}
			if (allColRefItem != -1) {
				this.accessedAllCols = (FormatableBitSet)saved[allColRefItem];
			}
			if(heapOnlyColRefItem!=-1){
				this.heapOnlyCols = (FormatableBitSet)saved[heapOnlyColRefItem];
			}
//            if(accessedAllCols!=null&&accessedHeapCols!=null){
//                accessFromTableCols = new FormatableBitSet(accessedAllCols);
//                for(int i= accessedHeapCols.anySetBit();i!=-1;i = accessedHeapCols.anySetBit(i)){
//                    accessFromTableCols.clear(i);
//                }
//            }
			// retrieve the array of columns coming from the index
			indexCols = ((ReferencedColumnsDescriptorImpl) saved[indexColMapItem]).getReferencedColumnPositions();			
			/* Get the result row template */
			resultRow = (ExecRow) generatedMethod.invoke(activation);
			
			compactRow = getCompactRow(activation.getLanguageConnectionContext(),resultRow, accessedAllCols, false);
			
			if (accessedHeapCols == null) {				
				rowArray = resultRow.getRowArray();
			}
			else {
				// Figure out how many columns are coming from the heap				
				final DataValueDescriptor[] resultRowArray = resultRow.getRowArray();
				final int heapOnlyLen = heapOnlyCols.getLength();

				// Need a separate DataValueDescriptor array in this case
				rowArray = new DataValueDescriptor[heapOnlyLen];
				final int minLen = Math.min(resultRowArray.length, heapOnlyLen);

				// Make a copy of the relevant part of rowArray
				for (int i = 0; i < minLen; ++i) {
					if (resultRowArray[i] != null && heapOnlyCols.isSet(i)) {
						rowArray[i] = resultRowArray[i];
					}
				}
				if (indexCols != null) {
					for (int index = 0; index < indexCols.length; index++) {
						if (indexCols[index] != -1) {
							compactRow.setColumn(index + 1,source.getExecRowDefinition().getColumn(indexCols[index] + 1));
						}
					}
				}			
			}
			SpliceLogUtils.trace(LOG,"accessedAllCols=%s,accessedHeapCols=%s,heapOnlyCols=%s,accessedCols=%s",accessedAllCols,accessedHeapCols,heapOnlyCols,accessedCols);
            SpliceLogUtils.trace(LOG,"rowArray=%s,compactRow=%s,resultRow=%s,resultSetNumber=%d",
                                            Arrays.asList(rowArray),compactRow,resultRow,resultSetNumber);
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Operation Init Failed!",e);
		}

	}
	
	@Override
	public NoPutResultSet executeScan() {
		SpliceLogUtils.trace(LOG,"executeScan");
		final List<SpliceOperation> operationStack = new ArrayList<SpliceOperation>();
		this.generateLeftOperationStack(operationStack);
		SpliceLogUtils.trace(LOG,"operationStack=%s",operationStack);
		SpliceOperation regionOperation = operationStack.get(0);
		SpliceLogUtils.trace(LOG,"regionOperation=%s",regionOperation);
		RowProvider provider;
		ExecRow template = getExecRowDefinition();
		if(regionOperation.getNodeTypes().contains(NodeType.REDUCE)&&this!=regionOperation){
			SpliceLogUtils.trace(LOG,"Scanning temp tables");
			provider = regionOperation.getReduceRowProvider(this,template);
		}else {
			SpliceLogUtils.trace(LOG,"scanning Map table");
			provider = regionOperation.getMapRowProvider(this,template);
		}
		return new SpliceNoPutResultSet(activation,this, provider);
	}

	@Override
	public SpliceOperation getLeftOperation() {
		SpliceLogUtils.trace(LOG,"getLeftOperation ",source);
		return this.source;
	}
	
	@Override
	public RowLocation getRowLocation() throws StandardException {
		return currentRowLocation;
	}

	@Override
	public ExecRow getCurrentRow() throws StandardException {
		return currentRow;
	}

	@Override
	public List<NodeType> getNodeTypes() {
		return Collections.singletonList(NodeType.SCAN);
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		SpliceLogUtils.trace(LOG,"getSubOperations");
		return Collections.singletonList(source);
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG,"getNextRowCore");
		ExecRow sourceRow;
		ExecRow retRow;
		boolean restrict = false;
		DataValueDescriptor restrictBoolean;

		do{
			sourceRow = source.getNextRowCore();
			SpliceLogUtils.trace(LOG,"retrieved index row %s",sourceRow);
			if(sourceRow!=null){
				baseRowLocation = (RowLocation)sourceRow.getColumn(sourceRow.nColumns());
				Get get =  SpliceUtils.createGet(baseRowLocation, rowArray,
						heapOnlyCols,
						Bytes.toBytes(transactionID));
				boolean rowExists = false;
				try{
					Result result = table.get(get);
					SpliceLogUtils.trace(LOG,"rowArray=%s,accessedHeapCols=%s,heapOnlyCols=%s,baseColumnMap=%s",
																		Arrays.toString(rowArray),accessedHeapCols,heapOnlyCols,Arrays.toString(baseColumnMap));
					rowExists = result!=null;
					if(rowExists){
						SpliceUtils.populate(result, compactRow.getRowArray(), accessedHeapCols,baseColumnMap);
					}
				}catch(IOException ioe){
					SpliceLogUtils.logAndThrowRuntime(LOG,ioe);
				}
				SpliceLogUtils.trace(LOG,"rowArray=%s,compactRow=%s",rowArray,compactRow);
				if(rowExists){
					if(!copiedFromSource){
						copiedFromSource=true;
						for(int index=0;index < indexCols.length;index++){
							if(indexCols[index] != -1) {
								SpliceLogUtils.trace(LOG,"indexCol overwrite for value " + indexCols[index]);												
								compactRow.setColumn(index+1,sourceRow.getColumn(indexCols[index]+1));
							}
						}
					}

					SpliceLogUtils.trace(LOG, "compactRow=%s", compactRow);
					setCurrentRow(compactRow);
					currentRowLocation = baseRowLocation;

					restrictBoolean = (DataValueDescriptor)
                            ((restriction == null) ? null: restriction.invoke(activation));
					restrict = (restrictBoolean ==null) ||
                            ((!restrictBoolean.isNull()) && restrictBoolean.getBoolean());
				}

				if(!restrict || !rowExists){
					clearCurrentRow();
					baseRowLocation = null;
					currentRowLocation=null;
				}else{
					currentRow = compactRow;
				}
				retRow = currentRow;

			}else{
				clearCurrentRow();
				baseRowLocation= null;
				retRow = null;
			}
		}while(sourceRow!=null && !restrict);
		SpliceLogUtils.trace(LOG, "emitting row %s",retRow);
		return retRow;
	}

	@Override
	public void close() throws StandardException {
		SpliceLogUtils.trace(LOG, "closing conglomerate controllers if necessary");
		source.close();
		super.close();
	}

	@Override
	public ExecRow getExecRowDefinition() {
		return compactRow.getClone();
	}
	
	@Override
	public String toString() {
		return String.format("IndexRowToBaseRow {source=%s,indexName=%s,conglomId=%d}",
                                                                        source,indexName,conglomId);
	}
}
