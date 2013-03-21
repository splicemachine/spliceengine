package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

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
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.storage.SimpleRegionAwareRowProvider;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.Accumulator;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.stats.TimingStats;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class GroupedAggregateOperation extends GenericAggregateOperation {	
	private static Logger LOG = Logger.getLogger(GroupedAggregateOperation.class);
	protected boolean isInSortedOrder;
	protected boolean isRollup;
	protected int orderingItem;
	protected int[] keyColumns;
	protected boolean[] descAscInfo;
	private int numDistinctAggs = 0;
	protected ColumnOrdering[] order;
	private List<ExecRow> finishedResults;
	private RingBuffer<ExecIndexRow> currentAggregations = new RingBuffer<ExecIndexRow>(1000); // TODO Make Configurable
	private ExecIndexRow[] resultRows;
	private HashSet<String>[][] distinctValues;
	private boolean completedExecution = false;

    protected RowProvider rowProvider;
    private Accumulator scanAccumulator = TimingStats.uniformAccumulator();

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
    	init(SpliceOperationContext.newContext(a));
    	recordConstructorTime();
    }
    
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		SpliceLogUtils.trace(LOG, "readExternal");
		super.readExternal(in);
		isInSortedOrder = in.readBoolean();
		isRollup = in.readBoolean();
		orderingItem = in.readInt();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		super.writeExternal(out);
		out.writeBoolean(isInSortedOrder);
		out.writeBoolean(isRollup);
		out.writeInt(orderingItem);
	}

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG, "init called");
		super.init(context);
		((SpliceOperation)source).init(context);
        GenericStorablePreparedStatement statement = context.getPreparedStatement();
		order = (ColumnOrdering[])
				((FormatableArrayHolder) (statement.getSavedObject(orderingItem))).getArray(ColumnOrdering.class);
		finishedResults = new ArrayList<ExecRow>();
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
//		if(isRollup)
//			resultRows = new ExecIndexRow[numGCols()+1];
//		else
//			resultRows = new ExecIndexRow[1];
		if(numDistinctAggs>0)
			distinctValues = (HashSet<String>[][])new HashSet[resultRows.length][aggregates.length];

		try {
            byte[] start = DerbyBytesUtil.generateBeginKeyForTemp(sequence[0]);
            byte[] finish = BytesUtil.copyAndIncrement(start);
			if(regionScanner==null){
				reduceScan = Scans.newScan(start,finish,transactionID);
//				reduceScan = SpliceUtils.generateScan(sequence[0],start,finish,transactionID);
//                rowProvider = new ScanRowProvider(regionScanner,sourceExecIndexRow);
			}else{
				Hasher hasher = new Hasher(sourceExecIndexRow.getRowArray(),keyColumns,null,sequence[0]);
				rowProvider = new SimpleRegionAwareRowProvider(
						transactionID,
                        context.getRegion(),
						regionScanner.getRegionInfo().getTableName(),
						HBaseConstants.DEFAULT_FAMILY_BYTES,
						start,finish,hasher,
						sourceExecIndexRow,null);
				rowProvider.open();
			}
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,"Unable to create reduce scan", e);
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,"Unable to create reduce scan", e);
		}
	}

	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow template){
        RowProvider provider = rowProvider;
        if(provider==null){
            SpliceUtils.setInstructions(reduceScan,activation,top);
            provider = new ClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,template,null);
        }
        return provider;
	}

	@Override		
	public SinkStats sink() {
		/*
		 * Sorts the data by sinking into the TEMP table. From there, the 
		 * getNextRowCore() method can be used to pull the data out in sequence and perform 
		 * the aggregation
		 */
        SinkStats.SinkAccumulator statsAccumulator = SinkStats.uniformAccumulator();
        statsAccumulator.start();
        SpliceLogUtils.trace(LOG, ">>>>statistics starts for sink for GroupedAggregation at "+statsAccumulator.getStartTime());
		SpliceLogUtils.trace(LOG, "sink");
		ExecRow row;
		HTableInterface tempTable = null;
		try{
			Put put;
			tempTable = SpliceAccessManager.getFlushableHTable(SpliceOperationCoprocessor.TEMP_TABLE);
			Hasher hasher = new Hasher(getExecRowDefinition().getRowArray(),keyColumns,null,sequence[0]);
            Serializer serializer = new Serializer();
            Accumulator sinkAccumulator = statsAccumulator.sinkAccumulator();
            do{
                row = doAggregation(false,statsAccumulator.processAccumulator());

                if(row==null)continue;

                long processStart = System.nanoTime();
                SpliceLogUtils.trace(LOG, "sinking row %s",row);
                put = Puts.buildInsert(hasher.generateSortedHashKey(row.getRowArray()),row.getRowArray(),null,serializer);
                tempTable.put(put);

                sinkAccumulator.tick(System.nanoTime() - processStart);
            }while(row!=null);
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
		
		SinkStats ss = statsAccumulator.finish();
		SpliceLogUtils.trace(LOG, ">>>>statistics finishes for sink for GroupedAggregation at "+statsAccumulator.getFinishTime());
        return ss;
	}
	
	@Override
	public void cleanup() { }

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG,"getNextRowCore");
		return doAggregation(true,scanAccumulator);
	}
	
	private final RingBuffer.Merger<ExecIndexRow> merger = new RingBuffer.Merger<ExecIndexRow>() {
		@Override
		public boolean shouldMerge(ExecIndexRow one, ExecIndexRow two){
			try {
				return sameGroupingValues(one,two) == numGCols();
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG,e);
				return false; //will never happen
			}
		}

		@Override
		public void merge(ExecIndexRow curr,ExecIndexRow next){
			try {
//				SpliceLogUtils.trace(LOG, "merging %s with %s",curr,next);
				mergeVectorAggregates(next,curr,-1);
//				SpliceLogUtils.trace(LOG,"merged result = %s",curr);
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG, e);
			}
		}
	};
	
	private ExecRow doAggregation(boolean useScan,Accumulator stats) throws StandardException {
		SpliceLogUtils.trace(LOG,"doAggregation start");
		
		//if we already have some results finished, just use the next one of those
		if(finishedResults.size()>0)
			return makeCurrent(finishedResults.remove(0));
		else if (completedExecution)
			return null; //we're finished, don't waste effort

		long start = System.nanoTime();
		
        /*
         * Lazily create the rollup result rows array to make sure that it's sized
         * appropriately on both sides of the MR boundary.
         */
        if(resultRows==null){
            if(isRollup&&!useScan){
                resultRows = new ExecIndexRow[numGCols()+1];
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
			SpliceLogUtils.trace(LOG,"nextRow=%s",nextRow);
            ExecIndexRow[] rolledUpRows = getRolledUpRows(nextRow,useScan);
            SpliceLogUtils.trace(LOG,"adding rolledUpRows %s", Arrays.toString(rolledUpRows));
            for(ExecIndexRow rolledUpRow:rolledUpRows){
                if(!useScan)
                    initializeVectorAggregation(rolledUpRow);
				if(!currentAggregations.merge(rolledUpRow,merger)){
//					SpliceLogUtils.trace(LOG, "found new results %s",rolledUpRow);
					ExecIndexRow row = (ExecIndexRow)rolledUpRow.getClone();

					ExecIndexRow finalized = currentAggregations.add(row);
					if(finalized!=null&&finalized !=row){
						return makeCurrent(finishAggregation(finalized));
					}
				}
            }
			nextRow = useScan? getNextRowFromScan():getNextRowFromSource();
            stats.tick(System.nanoTime()-start);
            start = System.nanoTime();
		}while(nextRow!=null);
		
		ExecRow next = finalizeResults();
		SpliceLogUtils.trace(LOG,"next aggregated row = %s",next);
		return next;
	}

    private ExecIndexRow[] getRolledUpRows(ExecIndexRow rowToRollUp, boolean scanned) throws StandardException {
        SpliceLogUtils.trace(LOG,"getRolledUpRows?"+isRollup);
        if(!isRollup||scanned){
            resultRows[0] = rowToRollUp;
            return resultRows;
        }

        int rollUpPos = numGCols();
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

	private int sameGroupingValues(ExecRow currRow,ExecRow newRow) 
												throws StandardException{
		for (int index = 0; index< numGCols();index++){
			DataValueDescriptor currOrderable = 
					currRow.getColumn(order[index].getColumnId()+1);
			DataValueDescriptor newOrderable = 
					newRow.getColumn(order[index].getColumnId()+1);
			if(!currOrderable.compare(DataValueDescriptor.ORDER_OP_EQUALS,
														newOrderable,true,true))
				return index;
		}
		return numGCols();
	}
	
	protected void initializeVectorAggregation(ExecRow row)
											throws StandardException{
//		SpliceLogUtils.trace(LOG,"initializing row %s",row);
		for(SpliceGenericAggregator aggregator: aggregates){
			aggregator.initialize(row);
			aggregator.accumulate(row, row);
		}
//		SpliceLogUtils.trace(LOG,"After initialization, row=%s",row);
	}
	
	private void mergeVectorAggregates(ExecRow newRow, ExecRow currRow, 
											int level) throws StandardException {
//		SpliceLogUtils.trace(LOG,"merging agg %s with %s",currRow, newRow);
		for(int i=0;i<aggregates.length;i++){
			SpliceGenericAggregator agg = aggregates[i];
//			if(agg.isDistinct()){
//				DataValueDescriptor newValue = agg.getInputColumnValue(newRow);
//				if (newValue.getString()!=null){
//					if(distinctValues[level][i].contains(newValue.getString()))
//						continue;
//					distinctValues[level][i].add(newValue.getString());
//				}
//			}
			agg.merge(newRow,currRow);
		}
//		SpliceLogUtils.trace(LOG,"agg row after merging = %s",currRow);
	}
	
	private int numGCols(){
		return order.length - numDistinctAggs;
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
			Result result = new Result(keyValues);
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
//		if(inputRow!=null)
//			initializeVectorAggregation(inputRow);
		return inputRow;
	}
	
	private ExecRow finalizeResults() throws StandardException {
		SpliceLogUtils.trace(LOG, "finalizeResults");
		completedExecution=true;
		for(ExecIndexRow row : currentAggregations){
			SpliceLogUtils.trace(LOG,"finishing Aggregation of row %s",row);
			finishedResults.add(finishAggregation(row));
		}
		currentAggregations.clear();
		if(finishedResults.size()>0)
			return makeCurrent(finishedResults.remove(0));
		else return null;
	}
	
	private <T extends ExecRow> ExecRow makeCurrent(T row) 
												throws StandardException{
		setCurrentRow(row);
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

	@Override
	public void openCore() throws StandardException {
		if(source!=null)source.openCore();
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
}
