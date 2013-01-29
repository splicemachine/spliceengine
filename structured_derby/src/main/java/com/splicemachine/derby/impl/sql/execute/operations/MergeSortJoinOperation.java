package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.storage.MergeSortRegionAwareRowProvider;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergeSortJoinOperation extends JoinOperation {
	private static Logger LOG = Logger.getLogger(MergeSortJoinOperation.class);
	protected String emptyRowFunMethodName;
	protected boolean wasRightOuterJoin;
	protected Qualifier[][] qualifierProbe;
	protected int leftHashKeyItem;
	protected int[] leftHashKeys;
	protected int rightHashKeyItem;
	protected int[] rightHashKeys;
	protected ExecRow rightTemplate;
	protected static List<NodeType> nodeTypes; 
	protected Scan reduceScan;
	protected enum JoinSide {RIGHT,LEFT};
	protected JoinSide joinSide;
	protected RowProvider clientProvider;
	protected MergeSortRegionAwareRowProvider serverProvider;
	protected SQLInteger rowType;
	protected byte[] priorHash;
	protected List<ExecRow> rights;
	protected byte[] rightHash;
	protected Iterator<ExecRow> rightIterator;
	
	static {
		nodeTypes = new ArrayList<NodeType>();
		nodeTypes.add(NodeType.REDUCE);
		nodeTypes.add(NodeType.SCAN);
		nodeTypes.add(NodeType.SINK);
	}
	
	public MergeSortJoinOperation() {
		super();
	}
	
	public MergeSortJoinOperation(NoPutResultSet leftResultSet,
			   int leftNumCols,
			   NoPutResultSet rightResultSet,
			   int rightNumCols,
			   int leftHashKeyItem,
			   int rightHashKeyItem,
			   Activation activation,
			   GeneratedMethod restriction,
			   int resultSetNumber,
			   boolean oneRowRightSide,
			   boolean notExistsRightSide,
			   double optimizerEstimatedRowCount,
			   double optimizerEstimatedCost,
			   String userSuppliedOptimizerOverrides) throws StandardException {		
				super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
						activation, restriction, resultSetNumber,oneRowRightSide, notExistsRightSide,
						optimizerEstimatedRowCount, optimizerEstimatedCost,userSuppliedOptimizerOverrides);
				SpliceLogUtils.trace(LOG, "instantiate");
				this.leftHashKeyItem = leftHashKeyItem;
				this.rightHashKeyItem = rightHashKeyItem;
				this.joinSide = JoinSide.LEFT;
                init(SpliceOperationContext.newContext(activation));
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
		SpliceLogUtils.trace(LOG, "readExternal");
		super.readExternal(in);
		leftHashKeyItem = in.readInt();
		rightHashKeyItem = in.readInt();
		joinSide = JoinSide.values()[in.readInt()];
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		super.writeExternal(out);
		out.writeInt(leftHashKeyItem);
		out.writeInt(rightHashKeyItem);
		out.writeInt(joinSide.ordinal());
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG, "getNextRowCore");
		if (rightIterator!= null && rightIterator.hasNext()) {
			currentRow = JoinUtils.getMergedRow(leftRow, rightIterator.next(), wasRightOuterJoin, this.leftNumCols, this.rightNumCols, mergedRow);
			this.setCurrentRow(currentRow);
			currentRowLocation = new HBaseRowLocation(SpliceUtils.getUniqueKey());
			SpliceLogUtils.trace(LOG, "current row returned %s",currentRow);
			return currentRow;
		}
		if (!serverProvider.hasNext()) {
			SpliceLogUtils.trace(LOG, "serverProvider exhausted");
			return null;
		}
		JoinSideExecRow joinRow = serverProvider.nextJoinRow();
		SpliceLogUtils.trace(LOG, "joinRow=%s",joinRow);
		if (joinRow == null) {
			SpliceLogUtils.trace(LOG, "serverProvider returned null rows");
			this.setCurrentRow(null);
			return null;
		}
		
		if (joinRow.getJoinSide().ordinal() == JoinSide.RIGHT.ordinal()) { // Right Side
			rightHash = joinRow.getHash();
			if (joinRow.sameHash(priorHash)) {
				SpliceLogUtils.trace(LOG, "adding additional right=%s",joinRow);
				rights.add(joinRow.getRow());
			} else {
				resetRightSide();
				SpliceLogUtils.trace(LOG, "adding initial right=%s",joinRow);
				rights.add(joinRow.getRow());
				priorHash = joinRow.getHash();
			}
			return getNextRowCore();
		} else { // Left Side
			leftRow = joinRow.getRow();
			if (joinRow.sameHash(priorHash)) {
				if (joinRow.sameHash(rightHash)) {
					SpliceLogUtils.trace(LOG, "initializing iterator with rights for left=%s",joinRow);
					rightIterator = rights.iterator();
					currentRow = JoinUtils.getMergedRow(leftRow, rightIterator.next(), wasRightOuterJoin, this.leftNumCols, this.rightNumCols, mergedRow);
					this.setCurrentRow(currentRow);
					currentRowLocation = new HBaseRowLocation(SpliceUtils.getUniqueKey());					
					SpliceLogUtils.trace(LOG, "current row returned %s",currentRow);
					return currentRow;
				} else {
					SpliceLogUtils.trace(LOG, "right hash miss left=%s",joinRow);
					resetRightSide();	
					priorHash = joinRow.getHash();
					return getNextRowCore();
				}
			} else {
				SpliceLogUtils.trace(LOG, "simple left with no right=%s",joinRow);
				resetRightSide();
				priorHash = joinRow.getHash();
				SpliceLogUtils.trace(LOG, "current row returned %s",currentRow);
				return getNextRowCore();			
			}			
		}
	}

	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow template){
        if(clientProvider==null){
            SpliceUtils.setInstructions(reduceScan,activation,top);
            clientProvider = new ClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,template,null);
        }
        return clientProvider;
	}

	private int[] generateHashKeys(int hashKeyItem) {
		FormatableArrayHolder fah = (FormatableArrayHolder)(activation.getPreparedStatement().getSavedObject(hashKeyItem));
		FormatableIntHolder[] fihArray = (FormatableIntHolder[]) fah.getArray(FormatableIntHolder.class);
		int[] keyColumns = new int[fihArray.length];
		for (int index = 0; index < fihArray.length; index++) {
			keyColumns[index] = fihArray[index].getInt();
		}
		return keyColumns;
	}
	
	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG, "init");
		super.init(context);
		try {
			leftHashKeys = generateHashKeys(leftHashKeyItem);
			rightHashKeys = generateHashKeys(rightHashKeyItem);
			mergedRow = activation.getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
			rightTemplate = activation.getExecutionFactory().getValueRow(rightNumCols);
			byte[] start = DerbyBytesUtil.generateBeginKeyForTemp(sequence[0]);
			byte[] finish = DerbyBytesUtil.generateEndKeyForTemp(sequence[0]);
			rowType = (SQLInteger) activation.getDataValueFactory().getNullInteger(null);
			Hasher leftHasher = new Hasher(leftRow.getRowArray(),leftHashKeys,null,sequence[0]); 
			Hasher rightHasher = new Hasher(rightRow.getRowArray(),rightHashKeys,null,sequence[0]); 
			if(regionScanner==null){
				reduceScan = Scans.newScan(start,finish,transactionID);
			}else{
				serverProvider = new MergeSortRegionAwareRowProvider(context.getRegion(),SpliceOperationCoprocessor.TEMP_TABLE,HBaseConstants.DEFAULT_FAMILY_BYTES,
						start,finish,leftHasher,leftRow,rightHasher,rightRow,null,rowType);		
				serverProvider.open();
			}
			} catch (IOException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG,"Unable to create reduce scan", e);
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG,"Unable to create reduce scan", e);
			}
	}
	
	@Override
	public void executeShuffle() throws StandardException {
		SpliceLogUtils.trace(LOG, "executeShuffle");
		joinSide = JoinSide.LEFT;
		OperationBranch operationBranch = new OperationBranch(getActivation(),getOperationStack(),leftResultSet.getExecRowDefinition());
		SpliceLogUtils.trace(LOG, "merge sort shuffling left");
		operationBranch.execCoprocessor();
		joinSide = JoinSide.RIGHT;
		SpliceLogUtils.trace(LOG, "merge sort shuffling right");
		operationBranch = new OperationBranch(getActivation(),getRightOperationStack(),rightResultSet.getExecRowDefinition());
		operationBranch.execCoprocessor();	
		SpliceLogUtils.trace(LOG, "shuffle finished");		
	}
	
	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG,"executeScan");
		final List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
		this.generateLeftOperationStack(opStack);
		SpliceLogUtils.trace(LOG,"operationStack=%s",opStack);
		
		// Get the topmost value, instead of the bottommost, in case it's you
		SpliceOperation regionOperation = opStack.get(opStack.size()-1); 
		SpliceLogUtils.trace(LOG,"regionOperation=%s",opStack);
		RowProvider provider;
		if (regionOperation.getNodeTypes().contains(NodeType.REDUCE)){
			provider = regionOperation.getReduceRowProvider(this,getExecRowDefinition());
		}else {
			provider = regionOperation.getMapRowProvider(this,getExecRowDefinition());
		}
		return new SpliceNoPutResultSet(activation,this,provider);
	}

	
	@Override		
	public long sink() {
		long numSunk=0l;
		SpliceLogUtils.trace(LOG, "sink with joinSide= %s",joinSide);
		ExecRow row = null;
		HTableInterface tempTable = null;
		try{
			Put put;
			Hasher hasher = null;
			tempTable = SpliceAccessManager.getFlushableHTable(SpliceOperationCoprocessor.TEMP_TABLE);
			NoPutResultSet resultSet = null;
			DataValueDescriptor[] additionalDescriptors = {activation.getDataValueFactory().getDataValue(joinSide.ordinal(), null)};
			switch (joinSide) {
				case LEFT: 
					hasher = new Hasher(leftResultSet.getExecRowDefinition().getRowArray(),leftHashKeys,null,sequence[0],additionalDescriptors,null);
					resultSet = leftResultSet;					
					break;
				case RIGHT: 
					hasher = new Hasher(rightResultSet.getExecRowDefinition().getRowArray(),rightHashKeys,null,sequence[0],additionalDescriptors,null);
					resultSet = rightResultSet;
					break;
			}			
			while ((row = resultSet.getNextRowCore())!=null){
			SpliceLogUtils.trace(LOG, "sinking row %s",row);
				byte[] rowKey = hasher.generateSortedHashKey(row.getRowArray(),additionalDescriptors);
				put = Puts.buildInsert(rowKey, row.getRowArray(),null, null, additionalDescriptors);
				put.setWriteToWAL(false); // Seeing if this speeds stuff up a bit...
				tempTable.put(put);
				numSunk++;
			}
			tempTable.flushCommits();
			tempTable.close();
		}catch (StandardException se){
			SpliceLogUtils.logAndThrowRuntime(LOG,se);
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		}finally{
			try {
				if(tempTable!=null)
					tempTable.close();
			} catch (IOException e) {
				SpliceLogUtils.error(LOG, "Unexpected error closing TempTable", e);
			}
		}
		return numSunk;
	}


	@Override
	public ExecRow getExecRowDefinition() {
		SpliceLogUtils.trace(LOG, "getExecRowDefinition");
		JoinUtils.getMergedRow(((SpliceOperation)this.leftResultSet).getExecRowDefinition(),((SpliceOperation)this.rightResultSet).getExecRowDefinition(),wasRightOuterJoin,rightNumCols,leftNumCols,mergedRow);
		return mergedRow;
	}
	
	@Override
	public List<NodeType> getNodeTypes() {
		SpliceLogUtils.trace(LOG, "getNodeTypes");
		return nodeTypes;
	}
	
	@Override
	public SpliceOperation getLeftOperation() {
		SpliceLogUtils.trace(LOG,"getLeftOperation");
		return leftResultSet;
	}

	protected void resetRightSide() {
		this.rights = new ArrayList<ExecRow>();
		this.rightIterator = null;
	}
}