package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils.JoinSide;
import com.splicemachine.derby.impl.storage.DistributedClientScanProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.*;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.hash.HashFunctions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MergeSortJoinOperation extends JoinOperation implements SinkingOperation {
    private static final long serialVersionUID = 2l;
	private static Logger LOG = Logger.getLogger(MergeSortJoinOperation.class);
    protected boolean wasRightOuterJoin;
    protected int leftHashKeyItem;
	protected int[] leftHashKeys;
	protected int rightHashKeyItem;
	protected int[] rightHashKeys;
	protected ExecRow rightTemplate;
	protected static List<NodeType> nodeTypes; 
	protected Scan reduceScan;
    protected SQLInteger rowType;
    public int emptyRightRowsReturned = 0;

    protected SpliceMethod<ExecRow> emptyRowFun;
    protected ExecRow emptyRow;

	static {
		nodeTypes = Arrays.asList(NodeType.REDUCE,NodeType.SCAN,NodeType.SINK);
	}

    private MergeSortJoiner joiner;

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
        if (spliceRuntimeContext.isLeft(resultSetNumber))
    		return leftResultSet.nextRow(spliceRuntimeContext);
    	return rightResultSet.nextRow(spliceRuntimeContext);
    }

    @Override
	public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        return next(false,spliceRuntimeContext);
    }

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        if(joiner!=null){
            joiner.close();
            joiner = null;
        }
    }

		protected ExecRow next(boolean outer, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "next");
				if(joiner==null){
						SpliceRuntimeContext left = SpliceRuntimeContext.generateLeftRuntimeContext(resultSetNumber);
						SpliceRuntimeContext right = SpliceRuntimeContext.generateRightRuntimeContext(resultSetNumber);
						if(spliceRuntimeContext.isSink()){
								left.markAsSink();
								right.markAsSink();
						}
						byte[] currentTaskId = spliceRuntimeContext.getCurrentTaskId();
						KeyDecoder leftKeyDecoder = getKeyEncoder(JoinSide.LEFT,leftHashKeys, currentTaskId).getDecoder();
						KeyHashDecoder leftRowDecoder = BareKeyHash.encoder(IntArrays.complement(leftHashKeys,leftNumCols),null).getDecoder();
						PairDecoder leftDecoder = new PairDecoder(leftKeyDecoder,leftRowDecoder,leftRow);

						KeyDecoder rightKeyDecoder = getKeyEncoder(JoinSide.RIGHT,rightHashKeys, currentTaskId).getDecoder();
						KeyHashDecoder rightRowDecoder = BareKeyHash.encoder(IntArrays.complement(rightHashKeys,rightNumCols),null).getDecoder();
						PairDecoder rightDecoder = new PairDecoder(rightKeyDecoder,rightRowDecoder,rightRow);

						StandardIterator<JoinSideExecRow> scanner = getMergeScanner(spliceRuntimeContext, leftDecoder, rightDecoder);
						scanner.open();
						Restriction mergeRestriction = getRestriction();
						joiner = getMergeJoiner(outer, scanner, mergeRestriction);
				}
				beginTime = getCurrentTimeMillis();
				boolean shouldClose = true;
				try{
						ExecRow joinedRow = joiner.nextRow();
						if(joinedRow!=null){
								rowsSeen++;
								shouldClose =false;
								setCurrentRow(joinedRow);
								if(currentRowLocation==null)
										currentRowLocation = new HBaseRowLocation();
                currentRowLocation.setValue(joiner.lastRowLocation());
                setCurrentRowLocation(currentRowLocation);
            }else{
                clearCurrentRow();
            }
            return joinedRow;
        }finally{
            if(shouldClose){
                if(LOG.isDebugEnabled()){
                    LOG.debug(String.format("Saw %s records (%s left, %s right)",
                            rowsSeen, joiner.getLeftRowsSeen(), joiner.getRightRowsSeen()));
                }
                joiner.close();
            }
        }
    }

    @Override
	public RowProvider getReduceRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        byte[] start = uniqueSequenceID;
        byte[] finish = BytesUtil.unsignedCopyAndIncrement(start);
        reduceScan = Scans.newScan(start,finish,SpliceUtils.NA_TRANSACTION_ID);
        if(failedTasks.size()>0){
            reduceScan.setFilter(new SuccessFilter(failedTasks));
        }
        if(top!=this && top instanceof SinkingOperation){
            SpliceUtils.setInstructions(reduceScan,activation,top,spliceRuntimeContext);
            return new DistributedClientScanProvider("mergeSortJoin",SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,decoder,spliceRuntimeContext);
        }else{
            //we need to scan the data directly on the client
            return RowProviders.openedSourceProvider(top,LOG,spliceRuntimeContext);
        }
	}

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return getReduceRowProvider(top,decoder,spliceRuntimeContext);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException{
        SpliceLogUtils.trace(LOG, "init");
        super.init(context);
        SpliceLogUtils.trace(LOG,"leftHashkeyItem=%d,rightHashKeyItem=%d",leftHashKeyItem,rightHashKeyItem);
        emptyRightRowsReturned = 0;
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);
        rightTemplate = activation.getExecutionFactory().getValueRow(rightNumCols);
        if(uniqueSequenceID!=null){
            byte[] start = new byte[uniqueSequenceID.length];
            System.arraycopy(uniqueSequenceID,0,start,0,start.length);
            byte[] finish = BytesUtil.unsignedCopyAndIncrement(start);
            rowType = (SQLInteger) activation.getDataValueFactory().getNullInteger(null);
            if(regionScanner==null)
                reduceScan = Scans.newScan(start,finish, getTransactionID());
            else{
                reduceScan = context.getScan();
            }
        }
	}


		@Override
		protected JobStats doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG, "executeShuffle");
				long start = System.currentTimeMillis();
				SpliceRuntimeContext spliceLRuntimeContext = SpliceRuntimeContext.generateLeftRuntimeContext(resultSetNumber);
				SpliceRuntimeContext spliceRRuntimeContext = SpliceRuntimeContext.generateRightRuntimeContext(resultSetNumber);
				ExecRow template = getExecRowDefinition();
				RowProvider leftProvider = leftResultSet.getMapRowProvider(this, OperationUtils.getPairDecoder(this,spliceLRuntimeContext),spliceLRuntimeContext);
				RowProvider rightProvider = rightResultSet.getMapRowProvider(this, OperationUtils.getPairDecoder(this,spliceRRuntimeContext),spliceRRuntimeContext);
				RowProvider combined = RowProviders.combine(leftProvider, rightProvider);
				SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,new SpliceRuntimeContext());
				JobStats stats = combined.shuffleRows(soi);
				nextTime+=System.currentTimeMillis()-start;
				return stats;
		}

		@Override
		public NoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG,"executeScan");
				final List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
				this.generateLeftOperationStack(opStack);
				SpliceLogUtils.trace(LOG,"operationStack=%s",opStack);

				ExecRow rowDef = getExecRowDefinition();
				RowProvider provider = getReduceRowProvider(this,OperationUtils.getPairDecoder(this,runtimeContext),runtimeContext);
				return new SpliceNoPutResultSet(activation,this,provider);
	}

		@Override
		public CallBuffer<KVPair> transformWriteBuffer(CallBuffer<KVPair> bufferToTransform) throws StandardException {
				return bufferToTransform;
		}

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				if(spliceRuntimeContext.isLeft(resultSetNumber)){
						return getKeyEncoder(JoinSide.LEFT,leftHashKeys,spliceRuntimeContext.getCurrentTaskId());
				}else
						return getKeyEncoder(JoinSide.RIGHT,rightHashKeys,spliceRuntimeContext.getCurrentTaskId());
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				if(spliceRuntimeContext.isLeft(resultSetNumber)){
						return BareKeyHash.encoder(IntArrays.complement(leftHashKeys,leftNumCols),null);
				}else
						return BareKeyHash.encoder(IntArrays.complement(rightHashKeys,rightNumCols),null);
		}

		private KeyEncoder getKeyEncoder(JoinSide joinSide,int[] keyColumns,byte[] taskId) throws StandardException{
				HashPrefix prefix = new BucketingPrefix(new FixedPrefix(uniqueSequenceID),
								HashFunctions.murmur3(0),
								SpliceDriver.driver().getTempTable().getCurrentSpread());
				final byte[] joinSideBytes = Encoding.encode(joinSide.ordinal());

				DataHash hash = BareKeyHash.encoder(keyColumns, null);

				/*
				 *The postfix looks like
				 *
				 * 0x00 <ordinal bytes> <uuid> <taskId>
				 *
				 * The last portion is just a unique postfix, so we extend that to prepend
				 * the join ordinal to it
				 */
				KeyPostfix keyPostfix =  new UniquePostfix(taskId){
						@Override
						public int getPostfixLength(byte[] hashBytes) throws StandardException {
								return 1+joinSideBytes.length+super.getPostfixLength(hashBytes);
						}

						@Override
						public void encodeInto(byte[] keyBytes, int postfixPosition, byte[] hashBytes) {
								keyBytes[postfixPosition]=0x00;
								System.arraycopy(joinSideBytes,0,keyBytes,postfixPosition+1,joinSideBytes.length);
								super.encodeInto(keyBytes, postfixPosition+1+joinSideBytes.length, hashBytes);
						}
				};

				return new KeyEncoder(prefix,hash,keyPostfix);
		}

		@Override
	public ExecRow getExecRowDefinition() throws StandardException {
		SpliceLogUtils.trace(LOG, "getExecRowDefinition");
		JoinUtils.getMergedRow((this.leftResultSet).getExecRowDefinition(), (this.rightResultSet).getExecRowDefinition(),
						wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
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

    @Override
    public String toString(){
        return "Merge"+super.toString();
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "MergeSortJoin:"+super.prettyPrint(indentLevel);
    }

    @Override
	public void	close() throws StandardException, IOException {
		SpliceLogUtils.trace(LOG, "close in MergeSortJoin");
		beginTime = getCurrentTimeMillis();

        if(joiner!=null)
            joiner.close();
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

/***********************************************************************************************************************************/
    /*private helper methods*/
    private StandardIterator<JoinSideExecRow> getMergeScanner(SpliceRuntimeContext spliceRuntimeContext,
																															PairDecoder leftDecoder,
																															PairDecoder rightDecoder) {
        StandardIterator<JoinSideExecRow> scanner;
        if(spliceRuntimeContext.isSink()){
            scanner = ResultMergeScanner.regionAwareScanner(reduceScan, transactionID, leftDecoder, rightDecoder, region);
        }else{
            scanner = ResultMergeScanner.clientScanner(reduceScan,leftDecoder,rightDecoder);
        }
        return scanner;
    }

    private MergeSortJoiner getMergeJoiner(boolean outer, final StandardIterator<JoinSideExecRow> scanner, final Restriction mergeRestriction) {
        if(outer){
            StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
                @Override
                public ExecRow get() throws StandardException {
                    if (emptyRow == null)
                        emptyRow = emptyRowFun.invoke();

                    return emptyRow;
                }
            };
            return new MergeSortJoiner(mergedRow,scanner,mergeRestriction,wasRightOuterJoin,leftNumCols,rightNumCols,
                    oneRowRightSide,notExistsRightSide, emptyRowSupplier){
                @Override
                protected boolean shouldMergeEmptyRow(boolean noRecordsFound) {
                    return noRecordsFound;
                }
            };
        }else{
            StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
                @Override
                public ExecRow get() throws StandardException {
                    return rightTemplate;
                }
            };
            return new MergeSortJoiner(mergedRow,scanner,mergeRestriction,wasRightOuterJoin,
                    leftNumCols,rightNumCols,oneRowRightSide, notExistsRightSide,emptyRowSupplier);
        }
    }

    private Restriction getRestriction() {
        Restriction mergeRestriction = Restriction.noOpRestriction;
        if(restriction!=null){
            mergeRestriction = new Restriction() {
                @Override
                public boolean apply(ExecRow row) throws StandardException {
                    activation.setCurrentRow(row,resultSetNumber);
                    DataValueDescriptor shouldKeep = restriction.invoke();
                    return !shouldKeep.isNull() && shouldKeep.getBoolean();
                }
            };
        }
        return mergeRestriction;
    }
}
