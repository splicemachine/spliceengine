package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.storage.MergeSortRegionAwareRowProvider;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.*;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergeSortJoinOperation extends JoinOperation {
    private static final long serialVersionUID = 2l;
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
	protected MergeSortNextRowIterator mergeSortIterator;
	public int emptyRightRowsReturned = 0;
	
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
                recordConstructorTime(); 
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
		SpliceLogUtils.trace(LOG, "readExternal");
		super.readExternal(in);
		leftHashKeyItem = in.readInt();
		rightHashKeyItem = in.readInt();
		joinSide = JoinSide.values()[in.readInt()];
		emptyRightRowsReturned = in.readInt();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		super.writeExternal(out);
		out.writeInt(leftHashKeyItem);
		out.writeInt(rightHashKeyItem);
		out.writeInt(joinSide.ordinal());
		out.writeInt(emptyRightRowsReturned);
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG, "getNextRowCore");
		beginTime = getCurrentTimeMillis();
		if (mergeSortIterator == null)
			mergeSortIterator = new MergeSortNextRowIterator(false);
		if (mergeSortIterator.hasNext()) {
			ExecRow next = mergeSortIterator.next();
			nextTime += getElapsedMillis(beginTime);
			return next;
		} else {
			setCurrentRow(null);
			return null;
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

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG, "init");
		super.init(context);
            SpliceLogUtils.trace(LOG,"leftHashkeyItem=%d,rightHashKeyItem=%d",leftHashKeyItem,rightHashKeyItem);
            emptyRightRowsReturned = 0;
			leftHashKeys = generateHashKeys(leftHashKeyItem, (SpliceBaseOperation) this.leftResultSet);
			rightHashKeys = generateHashKeys(rightHashKeyItem, (SpliceBaseOperation) this.rightResultSet);
			mergedRow = activation.getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
			rightTemplate = activation.getExecutionFactory().getValueRow(rightNumCols);
			byte[] start = DerbyBytesUtil.generateBeginKeyForTemp(sequence[0]);
			byte[] finish = BytesUtil.copyAndIncrement(start);
            rowType = (SQLInteger) activation.getDataValueFactory().getNullInteger(null);
			Hasher leftHasher = new Hasher(leftRow.getRowArray(),leftHashKeys,null,sequence[0]); 
			Hasher rightHasher = new Hasher(rightRow.getRowArray(),rightHashKeys,null,sequence[0]); 
			if(regionScanner==null){
				reduceScan = Scans.newScan(start,finish, getTransactionID());
			}else{
				serverProvider = new MergeSortRegionAwareRowProvider(getTransactionID(), context.getRegion(),SpliceOperationCoprocessor.TEMP_TABLE,SpliceConstants.DEFAULT_FAMILY_BYTES,
						start,finish,leftHasher,leftRow,rightHasher,rightRow,null,rowType);		
				serverProvider.open();
			}
	}
	
	@Override
	public void executeShuffle() throws StandardException {
		SpliceLogUtils.trace(LOG, "executeShuffle");
		long start = System.currentTimeMillis();
		joinSide = JoinSide.LEFT;
		OperationBranch operationBranch = new OperationBranch(getActivation(),getOperationStack(),leftResultSet.getExecRowDefinition());
		SpliceLogUtils.trace(LOG, "merge sort shuffling left");
		operationBranch.execCoprocessor(this.getClass().getName());
		joinSide = JoinSide.RIGHT;
		SpliceLogUtils.trace(LOG, "merge sort shuffling right");
		operationBranch = new OperationBranch(getActivation(),getRightOperationStack(),rightResultSet.getExecRowDefinition());
		operationBranch.execCoprocessor(this.getClass().getName());
		nextTime += System.currentTimeMillis() - start;
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
	public TaskStats sink() throws IOException {
        TaskStats.SinkAccumulator stats = TaskStats.uniformAccumulator();
        stats.start();
        SpliceLogUtils.trace(LOG, ">>>>statistics starts for sink for MergeSortJoin at "+stats.getStartTime());
		SpliceLogUtils.trace(LOG, "sink with joinSide= %s",joinSide);
		ExecRow row = null;
        CallBuffer<Mutation> writeBuffer;
		try{
			Put put;
			Hasher hasher = null;
            writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(SpliceOperationCoprocessor.TEMP_TABLE);
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
            Serializer serializer = new Serializer();

            do{
                long start = System.nanoTime();

                row = resultSet.getNextRowCore();
                if(row==null)continue;
                stats.readAccumulator().tick(System.nanoTime()-start);

                start = System.nanoTime();
                SpliceLogUtils.trace(LOG, "sinking row %s",row);
                byte[] rowKey = hasher.generateSortedHashKey(row.getRowArray(),additionalDescriptors);
                put = Puts.buildInsert(rowKey, row.getRowArray(),null, SpliceUtils.NA_TRANSACTION_ID, serializer, additionalDescriptors);
//                put.setWriteToWAL(false); // Seeing if this speeds stuff up a bit...
                writeBuffer.add(put);
                stats.writeAccumulator().tick(System.nanoTime()-start);
            }while(row!=null);
            writeBuffer.flushBuffer();
            writeBuffer.close();
		}catch (StandardException se){
			SpliceLogUtils.logAndThrowRuntime(LOG,se);
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		} catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG,Exceptions.getIOException(e));
        }
        //return stats.finish();
		TaskStats ss = stats.finish();
		SpliceLogUtils.trace(LOG, ">>>>statistics finishes for sink for MergeSortJoin at "+stats.getFinishTime());
        return ss;
	}

	private HTableInterface getBufferedTable() throws IOException {
		return makeBuffered(SpliceOperationCoprocessor.threadLocalEnvironment.get().getTable(SpliceOperationCoprocessor.TEMP_TABLE));
	}

	private HTableInterface makeBuffered(HTableInterface tableWrapper) {
		try {
			final Field tableField = tableWrapper.getClass().getDeclaredField("table");
			tableField.setAccessible(true);
			final HTable htable = (HTable) tableField.get(tableWrapper);
			htable.setAutoFlush(false);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		return tableWrapper;
	}

	@Override
	public ExecRow getExecRowDefinition() throws StandardException {
		SpliceLogUtils.trace(LOG, "getExecRowDefinition");
		JoinUtils.getMergedRow((this.leftResultSet).getExecRowDefinition(),(this.rightResultSet).getExecRowDefinition(),
                wasRightOuterJoin,rightNumCols,leftNumCols,mergedRow);
		return mergedRow;
	}

    private boolean areChildrenLeaves(){
        return leftResultSet instanceof ScanOperation && rightResultSet instanceof ScanOperation;
    }
 
   @Override
    public int[] getRootAccessedCols(long tableNumber) {

       int[] rootCols = null;

       if(leftResultSet.isReferencingTable(tableNumber)){
           rootCols = leftResultSet.getRootAccessedCols(tableNumber);
       }else if(rightResultSet.isReferencingTable(tableNumber)){
           int leftCols = getLeftNumCols();
           int[] rightRootCols = rightResultSet.getRootAccessedCols(tableNumber);
           rootCols = new int[rightRootCols.length];

           for(int i=0; i<rightRootCols.length; i++){
               rootCols[i] = rightRootCols[i] + leftCols;
           }

       }

       return rootCols;
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

    @Override
    public String toString(){
        return "Merge"+super.toString();
    }
	
	protected class MergeSortNextRowIterator implements Iterator<ExecRow> {
		protected JoinSideExecRow joinRow;
		protected boolean outerJoin;
		public MergeSortNextRowIterator(boolean outerJoin) {
			this.outerJoin = outerJoin;
		}

		@Override
		public boolean hasNext() {
			if (rightIterator!= null && rightIterator.hasNext()) {
				currentRow = JoinUtils.getMergedRow(leftRow, rightIterator.next(), wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
				setCurrentRow(currentRow);
				rowsReturned++;
				currentRowLocation = new HBaseRowLocation(SpliceUtils.getUniqueKey());
				SpliceLogUtils.trace(LOG, "current row returned %s", currentRow);
				return true;
			}
			if (!serverProvider.hasNext()) {
				SpliceLogUtils.trace(LOG, "serverProvider exhausted");
				return false;
			}
			while ( serverProvider.hasNext() && (joinRow = serverProvider.nextJoinRow()) != null) {
				if (joinRow.getJoinSide().ordinal() == JoinSide.RIGHT.ordinal()) { // Right Side
					rightHash = joinRow.getHash();
					if (joinRow.sameHash(priorHash)) {
						SpliceLogUtils.trace(LOG, "adding additional right=%s", joinRow);
						rights.add(joinRow.getRow().getClone());
					} else {
						resetRightSide();
						rowsSeenRight++;
						SpliceLogUtils.trace(LOG, "adding initial right=%s", joinRow);
						rights.add(joinRow.getRow().getClone());
						priorHash = joinRow.getHash();
					}
					continue;
				} 
				else { // Left Side
					leftRow = joinRow.getRow();
					rowsSeenLeft++;
					if (joinRow.sameHash(priorHash)) {
						if (joinRow.sameHash(rightHash)) {
							SpliceLogUtils.trace(LOG, "initializing iterator with rights for left=%s", joinRow);
							rightIterator = rights.iterator();
							currentRow = JoinUtils.getMergedRow(leftRow, rightIterator.next(), wasRightOuterJoin, rightNumCols,leftNumCols, mergedRow);
							setCurrentRow(currentRow);
							rowsReturned++;
							currentRowLocation = new HBaseRowLocation(SpliceUtils.getUniqueKey());					
							SpliceLogUtils.trace(LOG, "current row returned %s", currentRow);
							return true;
						} else {
							if (outerJoin) {
								SpliceLogUtils.trace(LOG, "simple left emit=%s", joinRow);
								resetRightSide();
								priorHash = joinRow.getHash();
								currentRow = JoinUtils.getMergedRow(leftRow, getEmptyRow(), wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
								setCurrentRow(currentRow);
								rowsReturned++;
								emptyRightRowsReturned++;
								return true;					
							} else {
								SpliceLogUtils.trace(LOG, "right hash miss left=%s", joinRow);
								resetRightSide();	
								priorHash = joinRow.getHash();
								continue;				
								
							}
						}
					} 
					else {
						if (outerJoin) {
							SpliceLogUtils.trace(LOG, "simple left with no right=%s", joinRow);
							resetRightSide();
							priorHash = joinRow.getHash();
							currentRow = JoinUtils.getMergedRow(leftRow, getEmptyRow(), wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
							setCurrentRow(currentRow);
							emptyRightRowsReturned++;
							rowsReturned++;
							return true;	
						} else {
							resetRightSide();
							priorHash = joinRow.getHash();
							SpliceLogUtils.trace(LOG, "current row returned %s", currentRow);
							continue;
						}
					}			
				}
			}
			SpliceLogUtils.trace(LOG, "serverProvider returned null rows");
			return false;
		}

		@Override
		public ExecRow next() {
			return currentRow;
		}

		@Override
		public void remove() {
			throw new RuntimeException("Cannot Be Removed - Not Implemented!");			
		}			
	}
	protected ExecRow getEmptyRow () {
		throw new RuntimeException("Should only be called on outer joins");
	}
	@Override
	public void	close() throws StandardException
	{
		SpliceLogUtils.trace(LOG, "close in MergeSortJoin");
		beginTime = getCurrentTimeMillis();

		if ( isOpen )
		{
			clearCurrentRow();
			super.close();
		}

		closeTime += getElapsedMillis(beginTime);
	}
}