package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.primitives.Bytes;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.storage.MergeSortRegionAwareRowProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.*;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

public class MergeSortJoinOperation extends JoinOperation implements SinkingOperation {
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

    private SpliceOperation resultSetToRead;

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
		nodeTypes = Arrays.asList(NodeType.REDUCE,NodeType.SCAN,NodeType.SINK);
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
                        activation, restriction, resultSetNumber, oneRowRightSide, notExistsRightSide,
                        optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides);
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
    public ExecRow getNextSinkRow() throws StandardException {
        if (resultSetToRead == null) {
            switch (joinSide) {
                case RIGHT:
                    resultSetToRead = rightResultSet;
                    break;
                case LEFT:
                    resultSetToRead = leftResultSet;
                    break;
            }
        }
        return resultSetToRead.getNextRowCore();
    }

    @Override
	public ExecRow getNextRowCore() throws StandardException {
        return next(false);
    }

    protected ExecRow next(boolean outer) throws StandardException {
        SpliceLogUtils.trace(LOG, "getNextRowCore");
        beginTime = getCurrentTimeMillis();
        if (mergeSortIterator == null)
            mergeSortIterator = new MergeSortNextRowIterator(outer);
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
            if(failedTasks.size()>0){
                reduceScan.setFilter(new SuccessFilter(failedTasks,false));
            }
            SpliceUtils.setInstructions(reduceScan,activation,top);
            clientProvider = new ClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,template,null);
        }
        return clientProvider;
	}

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        return getReduceRowProvider(top,template);
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
            serverProvider = new MergeSortRegionAwareRowProvider(getTransactionID(), context.getRegion(),
                    SpliceConstants.TEMP_TABLE_BYTES,SpliceConstants.DEFAULT_FAMILY_BYTES,
                    context.getScan(),leftHasher,leftRow,rightHasher,rightRow,null,rowType);
            serverProvider.open();
        }
	}

    @Override
    protected JobStats doShuffle() throws StandardException {
        SpliceLogUtils.trace(LOG, "executeShuffle");
        long start = System.currentTimeMillis();
        JoinSide oldSide = joinSide;

        ExecRow template = getExecRowDefinition();
        joinSide = JoinSide.LEFT;
        RowProvider leftProvider = leftResultSet.getMapRowProvider(this,template);

        joinSide = JoinSide.RIGHT;
        RowProvider rightProvider = rightResultSet.getMapRowProvider(this,template);

        RowProvider combined = RowProviders.combine(leftProvider, rightProvider);

        joinSide = oldSide;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this);
        JobStats stats = combined.shuffleRows(soi);
        nextTime+=System.currentTimeMillis()-start;
        return stats;
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
		RowProvider provider = getReduceRowProvider(this,getExecRowDefinition());
//		if (regionOperation.getNodeTypes().contains(NodeType.REDUCE)){
//			provider = regionOperation.getReduceRowProvider(this,getExecRowDefinition());
//		}else {
//			provider = regionOperation.getMapRowProvider(this,getExecRowDefinition());
//		}
		return new SpliceNoPutResultSet(activation,this,provider);
	}

    public OperationSink.Translator getTranslator() throws IOException {
        final Serializer serializer = Serializer.get();
        try {
            final DataValueDescriptor[] additionalDescriptors = {activation.getDataValueFactory().getDataValue(joinSide.ordinal(), null)};
            Hasher hasher = null;
            switch (joinSide) {
                case LEFT:
                    hasher = new Hasher(leftResultSet.getExecRowDefinition().getRowArray(),leftHashKeys,null,sequence[0],additionalDescriptors,null);
                    break;
                case RIGHT:
                    hasher = new Hasher(rightResultSet.getExecRowDefinition().getRowArray(),rightHashKeys,null,sequence[0],additionalDescriptors,null);
                    break;
            }
            final Hasher transHasher = hasher;
            final byte[][] keySet = new byte[2][];
            return new OperationSink.Translator() {
                @Nonnull
                @Override
                public List<Mutation> translate(@Nonnull ExecRow row, byte[] postfix) throws IOException {
                    try {
                        keySet[0] = transHasher.generateSortedHashKeyWithoutUniqueKey(row.getRowArray(),additionalDescriptors);
                        keySet[1] = postfix;
                        //TODO -sf- possible optimization here. Can we avoid writing duplicate rows at all when oneRowRightSide==true?
                        byte[] rowKey = Bytes.concat(keySet);
                        return Collections.<Mutation>singletonList(Puts.buildInsert(rowKey,
                                row.getRowArray(), null,
                                SpliceUtils.NA_TRANSACTION_ID, serializer, additionalDescriptors));
                    } catch (StandardException e) {
                        throw Exceptions.getIOException(e);
                    }
                }

                @Override
                public boolean mergeKeys() {
                    return false;
                }
            };
        } catch (StandardException e) {
            throw Exceptions.getIOException(e);
        }
    }

	@Override
	public ExecRow getExecRowDefinition() throws StandardException {
		SpliceLogUtils.trace(LOG, "getExecRowDefinition");
		JoinUtils.getMergedRow((this.leftResultSet).getExecRowDefinition(),(this.rightResultSet).getExecRowDefinition(),
                wasRightOuterJoin,rightNumCols,leftNumCols,mergedRow);
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

    @Override
    public String toString(){
        return "Merge"+super.toString();
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "MergeSortJoin:"+super.prettyPrint(indentLevel);
    }

    protected class MergeSortNextRowIterator implements Iterator<ExecRow> {
		protected boolean outerJoin;
		public MergeSortNextRowIterator(boolean outerJoin) {
			this.outerJoin = outerJoin;
		}


		@Override
		public boolean hasNext() {
            JoinSideExecRow joinRow;
            // If have remaining right rows to join with current left row
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
            // Process sorted rows from TEMP table, which are sorted by hash with rights coming before lefts
			while ( serverProvider.hasNext() && (joinRow = serverProvider.nextJoinRow()) != null ) {
                if (isRowRightSide(joinRow)){
                    handleRightRow(joinRow);
                    priorHash = rightHash = joinRow.getHash();
					continue;
				} else { // Left Side
					leftRow = joinRow.getRow();
					rowsSeenLeft++;
                    if (!joinRow.sameHash(priorHash)) {
                        priorHash = joinRow.getHash();
                    }

                    rightRow = getRightRowForLeft(joinRow);
                    if (rightRow != null){
                        currentRow = JoinUtils.getMergedRow(leftRow, rightRow, wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
                        setCurrentRow(currentRow);
                        rowsReturned++;
                        currentRowLocation = new HBaseRowLocation(SpliceUtils.getUniqueKey());
                        SpliceLogUtils.trace(LOG, "current row returned %s", currentRow);
                        return true;
                    } else {
                       continue;
                    }
				}
			}
			SpliceLogUtils.trace(LOG, "serverProvider returned null rows");
			return false;
		}

        public boolean isRowRightSide(JoinSideExecRow joinRow){
            return joinRow.getJoinSide().ordinal() == JoinSide.RIGHT.ordinal();
        }

        public void handleRightRow(JoinSideExecRow joinRow){
			if (joinRow.sameHash(priorHash)) {
                if (oneRowRightSide){
                    SpliceLogUtils.trace(LOG, "skipping additional right=%s", joinRow);
                } else {
                    SpliceLogUtils.trace(LOG, "adding additional right=%s", joinRow);
                    rights.add(joinRow.getRow().getClone());
                }
			} else {
				resetRightSide();
                // is this correct: only inc on the first?
				rowsSeenRight++;
				SpliceLogUtils.trace(LOG, "adding initial right=%s", joinRow);
				rights.add(joinRow.getRow().getClone());
			}
        }

        private ExecRow getRightRowForLeft(JoinSideExecRow leftJoinRow) {
            boolean matchingRights = leftJoinRow.sameHash(rightHash);
            if (matchingRights){
                if (notExistsRightSide) {
                    SpliceLogUtils.trace(LOG, "right antijoin miss for left=%s", leftJoinRow);
                    return null;
                }
                SpliceLogUtils.trace(LOG, "initializing iterator with rights for left=%s", leftJoinRow);
                rightIterator = rights.iterator();
                return rightIterator.next();
            } else {
                resetRightSide();
                if (notExistsRightSide || outerJoin) {
                    SpliceLogUtils.trace(LOG, "simple left emit=%s, outerJoin=%s, notExistsRightSide=%s",
                                            leftJoinRow, outerJoin, notExistsRightSide);
                    emptyRightRowsReturned++;
                    return outerJoin ? getEmptyRow() : rightTemplate;
                }
                SpliceLogUtils.trace(LOG, "right hash miss for left=%s", leftJoinRow);
                return null;
            }
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