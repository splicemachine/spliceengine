package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils.JoinSide;
import com.splicemachine.derby.impl.storage.DistributedClientScanProvider;
import com.splicemachine.derby.impl.storage.MergeSortRegionAwareRowProvider2;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.JoinSideExecRow;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.derby.utils.marshall.RowMarshall;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.SpliceLogUtils;

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
	protected MergeSortRegionAwareRowProvider2 serverProvider;
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
	
	public MergeSortJoinOperation(SpliceOperation leftResultSet,
			   int leftNumCols,
			   SpliceOperation rightResultSet,
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
                init(SpliceOperationContext.newContext(activation));
                recordConstructorTime(); 
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
		SpliceLogUtils.trace(LOG, "readExternal");
		super.readExternal(in);
		leftHashKeyItem = in.readInt();
		rightHashKeyItem = in.readInt();
		emptyRightRowsReturned = in.readInt();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		super.writeExternal(out);
		out.writeInt(leftHashKeyItem);
		out.writeInt(rightHashKeyItem);
		out.writeInt(emptyRightRowsReturned);
	}

    @Override
    public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
    	ExecRow nextSink;
    	if (spliceRuntimeContext.isLeft(resultSetNumber))
    		return leftResultSet.nextRow(spliceRuntimeContext);
    	return rightResultSet.nextRow(spliceRuntimeContext);
    }

    @Override
	public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return next(false,spliceRuntimeContext);
    }

    protected ExecRow next(boolean outer, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
    	SpliceLogUtils.trace(LOG, "nextRow");
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
	public RowProvider getReduceRowProvider(SpliceOperation top,RowDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        if(failedTasks.size()>0){
            reduceScan.setFilter(new SuccessFilter(failedTasks));
        }
        SpliceUtils.setInstructions(reduceScan,activation,top,spliceRuntimeContext);
        return new DistributedClientScanProvider("mergeSortJoin",SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,decoder,spliceRuntimeContext);
	}

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return getReduceRowProvider(top,decoder,spliceRuntimeContext);
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
        if(uniqueSequenceID!=null && !context.isOpSinking(this)){
            byte[] start = new byte[uniqueSequenceID.length];
            System.arraycopy(uniqueSequenceID,0,start,0,start.length);
            byte[] finish = BytesUtil.unsignedCopyAndIncrement(start);
            rowType = (SQLInteger) activation.getDataValueFactory().getNullInteger(null);
            if(regionScanner==null){
                reduceScan = Scans.newScan(start,finish, getTransactionID());
            }else{
                //get left-side decoder

            	SpliceRuntimeContext left = SpliceRuntimeContext.generateLeftRuntimeContext(resultSetNumber);
            	SpliceRuntimeContext right = SpliceRuntimeContext.generateRightRuntimeContext(resultSetNumber);
            	
                serverProvider = new MergeSortRegionAwareRowProvider2(getTransactionID(),context.getRegion(),
                        context.getScan(),
                        SpliceConstants.TEMP_TABLE_BYTES,
                        getRowEncoder(left,leftNumCols,leftHashKeys).getDual(leftResultSet.getExecRowDefinition()),
                        getRowEncoder(right,rightNumCols,rightHashKeys).getDual(rightResultSet.getExecRowDefinition()));
                serverProvider.open();
            }
        }
	}


    @Override
    protected JobStats doShuffle() throws StandardException {
        SpliceLogUtils.trace(LOG, "executeShuffle");
        long start = System.currentTimeMillis();
        SpliceRuntimeContext spliceLRuntimeContext = SpliceRuntimeContext.generateLeftRuntimeContext(resultSetNumber);
        SpliceRuntimeContext spliceRRuntimeContext = SpliceRuntimeContext.generateRightRuntimeContext(resultSetNumber);
        ExecRow template = getExecRowDefinition();
        RowProvider leftProvider = leftResultSet.getMapRowProvider(this, getRowEncoder(spliceLRuntimeContext).getDual(template),spliceLRuntimeContext);
        RowProvider rightProvider = rightResultSet.getMapRowProvider(this, getRowEncoder(spliceRRuntimeContext).getDual(template),spliceRRuntimeContext);
        RowProvider combined = RowProviders.combine(leftProvider, rightProvider);
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,new SpliceRuntimeContext());
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

        ExecRow rowDef = getExecRowDefinition();
        RowEncoder encoder = RowEncoder.create(rowDef.nColumns(),null,null,null,KeyType.BARE,RowMarshaller.packed());
		SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
        RowProvider provider = getReduceRowProvider(this,encoder.getDual(getExecRowDefinition()),spliceRuntimeContext);
		return new SpliceNoPutResultSet(activation,this,provider);
	}

    @Override
    public RowEncoder getRowEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
    	if (spliceRuntimeContext.isLeft(resultSetNumber))
                return getRowEncoder(spliceRuntimeContext,leftNumCols,leftHashKeys);
    	return getRowEncoder(spliceRuntimeContext,rightNumCols,rightHashKeys);
    }

    private RowEncoder getRowEncoder(SpliceRuntimeContext spliceRuntimeContext, final int numCols,int[] keyColumns) throws StandardException {
        int[] rowColumns;
        final byte[] joinSideBytes = Encoding.encode(spliceRuntimeContext.isLeft(resultSetNumber)?JoinSide.LEFT.ordinal():JoinSide.RIGHT.ordinal());
        KeyMarshall keyType = new KeyMarshall() {
            @Override
            public void encodeKey(DataValueDescriptor[] columns, int[] keyColumns,
                                  boolean[] sortOrder, byte[] keyPostfix, MultiFieldEncoder keyEncoder) throws StandardException {
                ((KeyMarshall)KeyType.BARE).encodeKey(columns,keyColumns,sortOrder,keyPostfix,keyEncoder);
                //add ordinal position
                /*
                 * add the ordinal position.
                 *
                 * We can safely call setRawBytes() here, because we know that joinSideBytes are encoded
                 * prior to being set here
                 */
                keyEncoder.setRawBytes(joinSideBytes);
                /*
                 * add a unique id
                 *
                 * We can safely call setRawBytes() here because we know that a unique key is 8 bytes, and it will
                 * never be decoded anyway
                 */
                keyEncoder.setRawBytes(SpliceUtils.getUniqueKey());
                /*
                 * add the postfix
                 *
                 * We can safely call setRawBytes() here because we know that the prefix will be a fixed length and
                 * will also never be outright decoded (it'll be used for correctness checking).
                 */
                keyEncoder.setRawBytes(keyPostfix);
            }

            @Override
            public void decode(DataValueDescriptor[] columns, int[] reversedKeyColumns, boolean[] sortOrder, MultiFieldDecoder rowDecoder) throws StandardException {
                /*
                 * Some Join columns have key sets like [0,0], where the same field is encoded multiple
                 * times. We need to only decode the first instance, or else we'll get incorrect answers
                 */
                rowDecoder.seek(11); //skip the query prefix
                int[] decodedColumns = new int[numCols];
                for(int key:reversedKeyColumns){
                    if(key==-1) continue;
                    if(decodedColumns[key]!=-1){
                        //we can decode this one, as it's not a duplicate
                        DerbyBytesUtil.decodeInto(rowDecoder,columns[key]);
                        decodedColumns[key] = -1;
                    }else{
                        //skip this one, it's a duplicate of something else
                        rowDecoder.skip();
                    }
                }
            }

            @Override
            public int getFieldCount(int[] keyColumns) {
                return ((KeyMarshall)KeyType.FIXED_PREFIX_UNIQUE_POSTFIX).getFieldCount(keyColumns)+1;
            }
        };
        RowMarshall rowType = RowMarshaller.packed();
        /*
         * Because there may be duplicate entries in keyColumns, we need to make sure
         * that rowColumns deals only with the unique form.
         */
        int[] allCols = new int[numCols];
        int numSet=0;
        for(int keyCol:keyColumns){
            int allCol = allCols[keyCol];
            if(allCol!=-1){
                //only set it if it hasn't already been set
                allCols[keyCol] = -1;
                numSet++;
            }
        }
        int pos=0;
        rowColumns = new int[numCols-numSet];
        for(int rowPos=0;rowPos<allCols.length;rowPos++){
            if(allCols[rowPos]!=-1){
                rowColumns[pos] = rowPos;
                pos++;
            }
        }

        return new RowEncoder(keyColumns,null,rowColumns,uniqueSequenceID,keyType,rowType, true);
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

    protected class MergeSortNextRowIterator {
		protected boolean outerJoin;
		public MergeSortNextRowIterator(boolean outerJoin) {
			this.outerJoin = outerJoin;
		}


		public boolean hasNext() throws StandardException {
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

        private ExecRow getRightRowForLeft(JoinSideExecRow leftJoinRow) throws StandardException {
            boolean matchingRights = leftJoinRow.sameHash(rightHash);
            // apply restriction
            int rightsMax = rights != null ? rights.size() : 0;
            List<ExecRow> filtered = new ArrayList<ExecRow>(rightsMax);
            if (matchingRights) {
                if (restriction == null) {
                    filtered = rights;
                } else {
                    ExecRow leftRow = leftJoinRow.getRow();
                    for (ExecRow right : rights) {
                        ExecRow merged = JoinUtils.getMergedRow(leftRow, right, wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
                        activation.setCurrentRow(merged, resultSetNumber());
                        DataValueDescriptor shouldKeep = restriction.invoke();
                        if (!shouldKeep.isNull() && shouldKeep.getBoolean()) {
                            filtered.add(right);
                        }
                    }
                }
            }
            if (filtered.size() > 0) {
                if (notExistsRightSide) {
                    SpliceLogUtils.trace(LOG, "right antijoin miss for left=%s", leftJoinRow);
                    return null;
                }
                SpliceLogUtils.trace(LOG, "initializing iterator with rights for left=%s", leftJoinRow);
                rightIterator = filtered.iterator();
                return rightIterator.next();
            } else {
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

		public ExecRow next() {
			return currentRow;
		}
	}

	protected ExecRow getEmptyRow () {
		throw new RuntimeException("Should only be called on outer joins");
	}
	@Override
	public void	close() throws StandardException, IOException {
		SpliceLogUtils.trace(LOG, "close in MergeSortJoin");
		beginTime = getCurrentTimeMillis();

		if ( isOpen )
		{
            //delete from the temp space
            if(reduceScan!=null)
                SpliceDriver.driver().getTempCleaner().deleteRange(uniqueSequenceID,reduceScan.getStartRow(),reduceScan.getStopRow());
            clearCurrentRow();
			super.close();
		}

		closeTime += getElapsedMillis(beginTime);
	}
}
