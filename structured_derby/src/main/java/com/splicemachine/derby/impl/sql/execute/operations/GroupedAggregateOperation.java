package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.google.common.primitives.Bytes;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.utils.*;
import com.splicemachine.job.JobStats;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.storage.SimpleRegionAwareRowProvider;
import com.splicemachine.derby.stats.Accumulator;
import com.splicemachine.derby.stats.TimingStats;
import com.splicemachine.utils.SpliceLogUtils;

import javax.annotation.Nonnull;

public class GroupedAggregateOperation extends GenericAggregateOperation {	
	private static Logger LOG = Logger.getLogger(GroupedAggregateOperation.class);
	protected boolean isInSortedOrder;
	protected boolean isRollup;
	protected int orderingItem;
	protected int[] keyColumns;
	protected boolean[] descAscInfo;
	private int numDistinctAggs = 0;
	protected ColumnOrdering[] order;
	private HashBuffer<ByteBuffer,ExecIndexRow> currentAggregations = new HashBuffer<ByteBuffer,ExecIndexRow>(SpliceConstants.ringBufferSize); 
	private ExecIndexRow[] resultRows;
	private HashSet<String>[][] distinctValues;
	private boolean completedExecution = false;
	private int numGCols;
    protected Hasher hasher;
    protected Serializer serializer;
    protected byte[][] keySet;
    protected RowProvider rowProvider;
    private Accumulator scanAccumulator = TimingStats.uniformAccumulator();
    /*used to determine whether or not to fetch from a scan*/
    private boolean isTemp;

    public GroupedAggregateOperation () {
    	super();
    	SpliceLogUtils.trace(LOG,"instantiate without parameters");
    }
  
    public GroupedAggregateOperation(NoPutResultSet s,
			boolean isInSortedOrder,
			int	aggregateItem,
			int	orderingItem,
			Activation a,
			GeneratedMethod ra,
			int maxRowSize,
			int resultSetNumber,
		    double optimizerEstimatedRowCount,
			double optimizerEstimatedCost,
			boolean isRollup) throws StandardException  {
    	super(s,aggregateItem,a,ra,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
    	SpliceLogUtils.trace(LOG, "instantiate with parameters");
    	this.isInSortedOrder = isInSortedOrder;
    	this.isRollup = isRollup;
    	this.orderingItem = orderingItem;
    	//get reduce scan
    	recordConstructorTime();
    }
    
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		isInSortedOrder = in.readBoolean();
		isRollup = in.readBoolean();
		orderingItem = in.readInt();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeBoolean(isInSortedOrder);
		out.writeBoolean(isRollup);
		out.writeInt(orderingItem);
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG, "init called");
		super.init(context);
		((SpliceOperation)source).init(context);
        GenericStorablePreparedStatement statement = context.getPreparedStatement();
		order = (ColumnOrdering[])
				((FormatableArrayHolder) (statement.getSavedObject(orderingItem))).getArray(ColumnOrdering.class);
		int localNumDistinctAggs = 0;
		for(SpliceGenericAggregator agg: aggregates){
			if(agg.isDistinct()){
				localNumDistinctAggs++;
			}
		}
		numDistinctAggs = localNumDistinctAggs;
		keyColumns = new int[order.length];
		descAscInfo = new boolean[order.length];
		for (int index = 0; index < order.length; index++) {
			keyColumns[index] = order[index].getColumnId();
			descAscInfo[index] = order[index].getIsAscending();
		}
		if(numDistinctAggs>0)
			distinctValues = (HashSet<String>[][])new HashSet[resultRows.length][aggregates.length];

            byte[] start = DerbyBytesUtil.generateBeginKeyForTemp(sequence[0]);
            byte[] finish = BytesUtil.copyAndIncrement(start);
		if(regionScanner==null){
               isTemp = true;
		}else{
				Hasher hasher = new Hasher(sourceExecIndexRow.getRowArray(),keyColumns,null,sequence[0]);
                rowProvider = new SimpleRegionAwareRowProvider(SpliceUtils.NA_TRANSACTION_ID,
                        context.getRegion(),
                        context.getScan(),
                        SpliceConstants.TEMP_TABLE_BYTES,
                        SpliceConstants.DEFAULT_FAMILY_BYTES,
                        hasher,
                        sourceExecIndexRow,null);
				rowProvider.open();
                isTemp = !context.isSink() || context.getTopOperation()!=this;
			}
			numGCols = order.length - numDistinctAggs;	
		    hasher = new Hasher(getExecRowDefinition().getRowArray(),keyColumns,null,sequence[0]);
		    serializer = new Serializer();
		    keySet = new byte[2][];
	}

	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow template) throws StandardException {
            try {
                reduceScan = Scans.buildPrefixRangeScan(sequence[0],SpliceUtils.NA_TRANSACTION_ID);
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
            SuccessFilter filter = new SuccessFilter(failedTasks,false);
            reduceScan.setFilter(filter);
            SpliceUtils.setInstructions(reduceScan, activation, top);
            return new ClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,template,null);
	}

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        return getReduceRowProvider(top,template);
    }

    @Override
    protected JobStats doShuffle() throws StandardException {
        long start = System.currentTimeMillis();
        final RowProvider rowProvider = ((SpliceOperation)source).getMapRowProvider(this, getExecRowDefinition());
        nextTime+= System.currentTimeMillis()-start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this);
        return rowProvider.shuffleRows(soi);
    }

    @Override
    public OperationSink.Translator getTranslator() throws IOException {

        return new OperationSink.Translator() {
            @Nonnull
            @Override
            public List<Mutation> translate(@Nonnull ExecRow row,byte[] postfix) throws IOException {
                    //keySet[0] = hasher.generateSortedHashKeyWithoutUniqueKey(row.getRowArray()); // Moved to getNextRowCore
            		keySet[1] = postfix;
                    byte[] rowKey = Bytes.concat(keySet);
                    Put put = Puts.buildTempTableInsert(rowKey,row.getRowArray(),null,serializer);
                    return Collections.<Mutation>singletonList(put);
            }

            @Override
            public boolean mergeKeys() {
                return false; //need to make sure we're unique within regions
            }
        };
    }


	@Override
	public void cleanup() { 
		
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG,"getNextRowCore");
		return doAggregation(isTemp,scanAccumulator);
	}
	
	private final HashBuffer.Merger<ByteBuffer,ExecIndexRow> merger = new HashBuffer.Merger<ByteBuffer,ExecIndexRow>() {
		@Override
		public ExecIndexRow shouldMerge(ByteBuffer key){
			return currentAggregations.get(key);
		}

		@Override
		public void merge(ExecIndexRow curr,ExecIndexRow next){
			try {
				mergeVectorAggregates(next,curr,-1);
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG, e);
			}
		}
	};
	
	private ExecRow doAggregation(boolean useScan,Accumulator stats) throws StandardException {		
		//if we already have some results finished, just use the next one of those
		if (completedExecution) {
			if (currentAggregations.size()>0) {
				ByteBuffer key = currentAggregations.keySet().iterator().next();
				return makeCurrent(key.array(),currentAggregations.remove(key));
			}
			else 
				return null; // Done
		}
		long start = System.nanoTime();
		
        /*
         * Lazily create the rollup result rows array to make sure that it's sized
         * appropriately on both sides of the MR boundary.
         */
        if(resultRows==null){
            if(isRollup&&!useScan){
                resultRows = new ExecIndexRow[numGCols+1];
            }else{
                resultRows = new ExecIndexRow[1];
            }
        }
		//get the next row. if it's null, we've finished reading results, so use what we already have
		ExecIndexRow nextRow = useScan? getNextRowFromScan():getNextRowFromSource();
		if(nextRow ==null) {
            return finalizeResults();
        }
		do{
			//the next row pulled isn't empty, so have to process it
			//SpliceLogUtils.trace(LOG,"nextRow=%s",nextRow);
            ExecIndexRow[] rolledUpRows = getRolledUpRows(nextRow,useScan);
            //SpliceLogUtils.trace(LOG,"adding rolledUpRows %s", Arrays.toString(rolledUpRows));
            for(ExecIndexRow rolledUpRow:rolledUpRows) {
                if(!useScan)
                    initializeVectorAggregation(rolledUpRow);                
                keySet[0] = hasher.generateSortedHashKeyWithoutUniqueKey(rolledUpRow.getRowArray()); 
				if(!currentAggregations.merge(ByteBuffer.wrap(keySet[0]),rolledUpRow,merger)){
					ExecIndexRow row = (ExecIndexRow)rolledUpRow.getClone();
					ExecIndexRow finalized = currentAggregations.add(ByteBuffer.wrap(keySet[0]),row);
					if(finalized!=null&&finalized !=row){
						return makeCurrent(keySet[0],finishAggregation(finalized));
					}
				}
            }
			nextRow = useScan? getNextRowFromScan():getNextRowFromSource();
            stats.tick(System.nanoTime()-start);
            start = System.nanoTime();
		}while(nextRow!=null);
		
		ExecRow next = finalizeResults();
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG,"next aggregated row = %s",next);
		return next;
	}

    private ExecIndexRow[] getRolledUpRows(ExecIndexRow rowToRollUp, boolean scanned) throws StandardException {
        if(!isRollup||scanned){
            resultRows[0] = rowToRollUp;
            return resultRows;
        }

        int rollUpPos = numGCols;
        int pos = 0;
        ExecIndexRow nextRow =  (ExecIndexRow)rowToRollUp.getClone();
        SpliceLogUtils.trace(LOG,"setting rollup cols to null");
        do{
            SpliceLogUtils.trace(LOG,"adding row %s",nextRow);
            resultRows[pos] = nextRow;

            //strip out the next key in the rollup
            if(rollUpPos>0){
	            nextRow = (ExecIndexRow)nextRow.getClone();
	            DataValueDescriptor rollUpCol = nextRow.getColumn(order[rollUpPos-1].getColumnId()+1);
	            rollUpCol.setToNull();
            }
            rollUpPos--;
            pos++;
        }while(rollUpPos>=0);

        return resultRows;
    }
	
	protected void initializeVectorAggregation(ExecRow row) throws StandardException{
	   for(SpliceGenericAggregator aggregator: aggregates){
			aggregator.initialize(row);
			aggregator.accumulate(row, row);
		}
	}
	
	private void mergeVectorAggregates(ExecRow newRow, ExecRow currRow, int level) throws StandardException {
		for(SpliceGenericAggregator agg : aggregates){
			agg.merge(newRow,currRow);
		}
	}
	
	protected ExecIndexRow getNextRowFromScan() throws StandardException {
		SpliceLogUtils.trace(LOG,"getting next row from scan");
		if(rowProvider!=null){
			if(rowProvider.hasNext())
				return (ExecIndexRow)rowProvider.next();
			else return null;
		}else{
			List<KeyValue> keyValues = new ArrayList<KeyValue>();
			try{
				regionScanner.next(keyValues);
			}catch(IOException ioe){
				SpliceLogUtils.logAndThrow(LOG, 
						StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,ioe));
			}
			Result result = new Result(keyValues); // XXX - TODO FIX JLEACH
			if(keyValues.isEmpty())return null;
			
			else{
				ExecIndexRow row = (ExecIndexRow)sourceExecIndexRow.getClone();
				SpliceUtils.populate(result, null, row.getRowArray());
				return row;
			}
		}
	}
	
	private ExecIndexRow getNextRowFromSource() throws StandardException{
		ExecRow sourceRow;
		ExecIndexRow inputRow = null;
		
		if ((sourceRow = source.getNextRowCore())!=null){
			sourceExecIndexRow.execRowToExecIndexRow(sourceRow);
			inputRow = sourceExecIndexRow;
		}
		return inputRow;
	}
	
	private ExecRow finalizeResults() throws StandardException {
		SpliceLogUtils.trace(LOG, "finalizeResults");
		completedExecution=true;
		currentAggregations = currentAggregations.finishAggregates(aggregateFinisher);
		if(currentAggregations.size()>0) {
			ByteBuffer key = currentAggregations.keySet().iterator().next();
			return makeCurrent(key.array(),currentAggregations.remove(key));
		}
		else 
			return null;
	}
	
	private <T extends ExecRow> ExecRow makeCurrent(byte[] key, T row) throws StandardException{
		setCurrentRow(row);
        keySet[0] = key;
		return row;
	}

	@Override
	public ExecRow getExecRowDefinition() {
		SpliceLogUtils.trace(LOG,"getExecRowDefinition");
		ExecRow row = sourceExecIndexRow.getClone();
		return row;
	}

	@Override
	public String toString() {
		return "GroupedAggregateOperation {source="+source;
	}


	public boolean isInSortedOrder() {
		return this.isInSortedOrder;
	}
	
	public boolean hasDistinctAggregate() {
		return this.numDistinctAggs>0;
	}
	
	@Override
	public long getTimeSpent(int type)
	{
		long totTime = constructorTime + openTime + nextTime + closeTime;

		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
		else
			return totTime;
	}
	@Override
	public void	close() throws StandardException
	{
		SpliceLogUtils.trace(LOG, "close in GroupedAggregate");
		beginTime = getCurrentTimeMillis();
		if ( isOpen )
	    {
			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
		    clearCurrentRow();
			sourceExecIndexRow = null;
			source.close();

			super.close();
		}
		closeTime += getElapsedMillis(beginTime);

		isOpen = false;
	}
	
	public Properties getSortProperties() {
		Properties sortProperties = new Properties();
		sortProperties.setProperty("numRowsInput", ""+getRowsInput());
		sortProperties.setProperty("numRowsOutput", ""+getRowsOutput());
		return sortProperties;
	}

    @Override
    public String prettyPrint(int indentLevel) {
        return "Grouped"+super.prettyPrint(indentLevel);
    }
}
