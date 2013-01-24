package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
/**
 * Operation for performing Scalar Aggregations (sum, avg, max/min, etc.). 
 *  
 * @author Scott Fines
 *
 */
public class ScalarAggregateOperation extends GenericAggregateOperation {	
	private static Logger LOG = Logger.getLogger(ScalarAggregateOperation.class);
	
	protected boolean isInSortedOrder;
	protected boolean singleInputRow;
	protected int countOfRows;
	protected int rowsInput = 0;
	
	protected boolean isOpen=false;
	private boolean nextSatisfied;
	private long regionId;
	
    public ScalarAggregateOperation () {
    	super();
    }
  
    public ScalarAggregateOperation(NoPutResultSet s,
			boolean isInSortedOrder,
			int	aggregateItem,
			Activation a,
			GeneratedMethod ra,
			int resultSetNumber,
			boolean singleInputRow,
		    double optimizerEstimatedRowCount,
		    double optimizerEstimatedCost) throws StandardException  {
    	super(s,aggregateItem,a,ra,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
    	this.isInSortedOrder = isInSortedOrder;
    	this.singleInputRow = singleInputRow;
    	
    	ExecutionFactory factory = a.getExecutionFactory();
    	sortTemplateRow = factory.getIndexableRow((ExecRow)rowAllocator.invoke(a));
		sourceExecIndexRow = factory.getIndexableRow(sortTemplateRow);
		try {
			this.reduceScan = SpliceUtils.generateScan(sequence[0], DerbyBytesUtil.generateBeginKeyForTemp(sequence[0]), 
					DerbyBytesUtil.generateEndKeyForTemp(sequence[0]), transactionID);
		} catch(IOException e){
			SpliceLogUtils.logAndThrowRuntime(LOG,"Unable to get Region ids",e);
		}
    }
    
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		SpliceLogUtils.trace(LOG,"readExternal");
		super.readExternal(in);
		isInSortedOrder = in.readBoolean();
		singleInputRow = in.readBoolean();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG,"writeExternal");
		super.writeExternal(out);
		out.writeBoolean(isInSortedOrder);
		out.writeBoolean(singleInputRow);
	}

	@Override
	public void openCore() throws StandardException {
		SpliceLogUtils.trace(LOG,"openCore");
		source.openCore();
		isOpen=true;
	}
	
	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow template){
		SpliceUtils.setInstructions(reduceScan,activation,top);
		return new ClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,template,null);
	}

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"init");
		super.init(context);
    	ExecutionFactory factory = activation.getExecutionFactory();
    	if(regionScanner!=null)
    		this.regionId = regionScanner.getRegionInfo().getRegionId();
    	try {
			sortTemplateRow = factory.getIndexableRow((ExecRow)rowAllocator.invoke(activation));
			sourceExecIndexRow = factory.getIndexableRow(sortTemplateRow);
		
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG,"getNextRowCore");
		return doAggregation(true);
	}

	protected ExecRow doAggregation(boolean useScan) throws StandardException{
		ExecIndexRow execIndexRow = null;
		ExecIndexRow aggResult = null;
		if(useScan){
			while ((execIndexRow = getNextRowFromScan(false))!=null){
				SpliceLogUtils.trace(LOG,"aggResult =%s before",aggResult);
				aggResult = aggregate(execIndexRow,aggResult,false);
				SpliceLogUtils.trace(LOG,"aggResult =%s after",aggResult);
			}
		}else{
			while ((execIndexRow = getNextRowFromSource(false))!=null){
				aggResult = aggregate(execIndexRow,aggResult,true);
			}
		}
		SpliceLogUtils.trace(LOG, "aggResult=%s",aggResult);
		if(aggResult==null) return null; //we didn't have any rows to aggregate
		if(countOfRows==0){
			aggResult = finishAggregation(aggResult);
			setCurrentRow(aggResult);
			countOfRows++;
		}
		return aggResult;
	}
	
	protected ExecIndexRow aggregate(ExecIndexRow execIndexRow, 
									ExecIndexRow aggResult, boolean doInitialize) throws StandardException{
		if(aggResult==null){
			aggResult = (ExecIndexRow)execIndexRow.getClone();
			SpliceLogUtils.trace(LOG, "aggResult = %s aggregate before",aggResult);
			if(doInitialize){
				initializeScalarAggregation(aggResult);
				SpliceLogUtils.trace(LOG, "aggResult = %s aggregate after",aggResult);
			}
		}else
			accumulateScalarAggregation(execIndexRow, aggResult, false);
		return aggResult;
	}
	
	protected ExecIndexRow getNextRowFromScan(boolean doClone) throws StandardException {
		//TODO -sf- make sure that only one scanner does the final aggregation 
		SpliceLogUtils.trace(LOG, "getting next result from TEMP Table");
		List<KeyValue> keyValues = new ArrayList<KeyValue>();
		try{
			regionScanner.next(keyValues);
		}catch(IOException ioe){
			SpliceLogUtils.logAndThrow(LOG, 
					StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION, ioe));
		}
		Result result = new Result(keyValues);
		if(keyValues.isEmpty())
			return null;
		else{
			ExecIndexRow row = (ExecIndexRow)sourceExecIndexRow.getClone();
			SpliceUtils.populate(result, null,row.getRowArray());
			SpliceLogUtils.trace(LOG, "returned row = %s",row);
			return row;
		}
	}
	
	private ExecIndexRow getNextRowFromSource(boolean doClone) throws StandardException{
		SpliceLogUtils.trace(LOG,"getNextRowFromSource");
		ExecRow sourceRow;
		ExecIndexRow inputRow = null;
		if((sourceRow = source.getNextRowCore())!=null){
			SpliceLogUtils.trace(LOG,"sourceRow=%s",sourceRow);
			rowsInput++;
			sourceExecIndexRow.execRowToExecIndexRow(doClone? sourceRow.getClone():sourceRow);
			inputRow = sourceExecIndexRow;
		}
		return inputRow;
	}
	
	@Override
	public ExecRow getExecRowDefinition(){
		SpliceLogUtils.trace(LOG,"getExecRowDefinition");
		ExecRow row = sourceExecIndexRow.getClone();
		DataValueDescriptor[] descs = new DataValueDescriptor[aggregates.length];
		try {
			int i=0;
			for(SpliceGenericAggregator agg:aggregates){
				SpliceLogUtils.trace(LOG, "aggColNum=%d",agg.getAggregatorInfo().getAggregatorColNum());
				DataValueDescriptor desc = row.getColumn(agg.getAggregatorInfo().getAggregatorColNum());
				descs[i] = desc;
				i++;
			}
			row.setRowArray(descs);
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
		return row;
	}

	protected void initializeScalarAggregation(ExecRow aggResult) throws StandardException{
		for(SpliceGenericAggregator aggregator: aggregates){
			SpliceLogUtils.trace(LOG, "BEFORE INITIALIZATION: aggResult = %s",aggResult);
			aggregator.initialize(aggResult);
			//SpliceLogUtils.trace(LOG, "BEFORE FIRST ACCUMULATE: aggResult = %s",aggResult);
			aggregator.accumulate(aggResult,aggResult);
			SpliceLogUtils.trace(LOG, "AFTER FIRST ACCUMULATE: aggResult = %s",aggResult);
		}
	}
	
	protected void accumulateScalarAggregation(ExecRow nextRow,
									ExecRow aggResult,
									boolean hasDistinctAggregates) throws StandardException{
		int size = aggregates.length;
		for(int i=0;i<size;i++){
			SpliceGenericAggregator currAggregate = aggregates[i];
			if(hasDistinctAggregates &&
					!currAggregate.isDistinct()){
				currAggregate.merge(nextRow, aggResult);
			}else{
				currAggregate.accumulate(nextRow,aggResult);
			}
		}
	}
	
	@Override
	public long sink() {
		long numSunk=0l;
		SpliceLogUtils.trace(LOG, "sink");
		ExecRow row = null;
		try{
			Put put;
			HTableInterface tempTable = SpliceAccessManager.getHTable(SpliceOperationCoprocessor.TEMP_TABLE);
			while((row = doAggregation(false)) !=null){
				SpliceLogUtils.trace(LOG,"row="+row);
				put = SpliceUtils.insert(row.getRowArray(), 
						DerbyBytesUtil.generateRegionHashKey(regionId, sequence[0]),null);
				SpliceLogUtils.trace(LOG, "put=%s",put);
				tempTable.put(put);
			}
			tempTable.close();
			numSunk++;
		}catch(StandardException se){
			SpliceLogUtils.logAndThrowRuntime(LOG, se);
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
		return numSunk;
	}

	@Override
	public String toString() {
		return "ScalarAggregateOperation {source=" + source + "}";
	}
	
	
}