package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.primitives.Bytes;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ProvidesDefaultClientScanProvider;
import com.splicemachine.derby.utils.*;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.derby.utils.marshall.RowType;
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
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
/**
 * Operation for performing Scalar Aggregations (sum, avg, max/min, etc.). 
 *  
 * @author Scott Fines
 *
 */
public class ScalarAggregateOperation extends GenericAggregateOperation {
	public static long serialVersionUID = 1l;
	private static Logger LOG = Logger.getLogger(ScalarAggregateOperation.class);
	
	protected boolean isInSortedOrder;
	protected boolean singleInputRow;
	protected int countOfRows;
	protected int rowsInput = 0;

	protected boolean isOpen=false;

    private RowDecoder scanDecoder;

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
        super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.isInSortedOrder = isInSortedOrder;
        this.singleInputRow = singleInputRow;

        ExecutionFactory factory = a.getExecutionFactory();
        sortTemplateRow = factory.getIndexableRow((ExecRow)rowAllocator.invoke(a));
        sourceExecIndexRow = factory.getIndexableRow(sortTemplateRow);
        recordConstructorTime();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        isInSortedOrder = in.readBoolean();
        singleInputRow = in.readBoolean();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeBoolean(isInSortedOrder);
		out.writeBoolean(singleInputRow);
	}

	@Override
	public void openCore() throws StandardException {
        super.openCore();
		source.openCore();
		isOpen=true;
	}
	
	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,RowDecoder rowDecoder) throws StandardException {
        try {
            reduceScan = Scans.buildPrefixRangeScan(sequence[0], SpliceUtils.NA_TRANSACTION_ID);
            //make sure that we filter out failed tasks
            SuccessFilter filter = new SuccessFilter(failedTasks,false);
            reduceScan.setFilter(filter);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        SpliceUtils.setInstructions(reduceScan,activation,top);
        return new ProvidesDefaultClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,rowDecoder);
	}

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder rowDecoder) throws StandardException {
        return ((SpliceOperation)source).getMapRowProvider(top,rowDecoder);
    }

    @Override
	public void init(SpliceOperationContext context) throws StandardException{
		super.init(context);
		ExecutionFactory factory = activation.getExecutionFactory();
		try {
			sortTemplateRow = factory.getIndexableRow((ExecRow)rowAllocator.invoke(activation));
			sourceExecIndexRow = factory.getIndexableRow(sortTemplateRow);
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
	}

    @Override
    public ExecRow getNextSinkRow() throws StandardException {
        return doAggregation(false);
    }

    @Override
	public ExecRow getNextRowCore() throws StandardException {
		return doAggregation(true);
	}

	protected ExecRow doAggregation(boolean useScan) throws StandardException{
		ExecIndexRow execIndexRow;
		ExecIndexRow aggResult = null;
		if(useScan){
            do{
                execIndexRow = getNextRowFromScan(false);
                if(execIndexRow==null)continue;
                aggResult = aggregate(execIndexRow,aggResult,false,true);
            }while(execIndexRow!=null);
		}else{
            do{
                execIndexRow = getNextRowFromSource(false);
                if(execIndexRow==null) continue;
                aggResult = aggregate(execIndexRow,aggResult,true,false);
            }while(execIndexRow!=null);
		}
		if(aggResult==null) return null; //we didn't have any rows to aggregate
		if(countOfRows==0){
			aggResult = finishAggregation(aggResult);
			setCurrentRow(aggResult);
			countOfRows++;
		}
		return aggResult;
	}
	
	protected ExecIndexRow aggregate(ExecIndexRow execIndexRow, 
									ExecIndexRow aggResult, boolean doInitialize, boolean isScan) throws StandardException{
		if(aggResult==null){
			aggResult = (ExecIndexRow)execIndexRow.getClone();
			if(doInitialize){
				initializeScalarAggregation(aggResult);
			}
		}else
			accumulateScalarAggregation(execIndexRow, aggResult, false,isScan);
		return aggResult;
	}
	
	protected ExecIndexRow getNextRowFromScan(boolean doClone) throws StandardException {
        if(scanDecoder==null)
            scanDecoder = getRowEncoder().getDual(sourceExecIndexRow,true);
		List<KeyValue> keyValues = new ArrayList<KeyValue>();
		try{
			regionScanner.next(keyValues);
		}catch(IOException ioe){
			SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION, ioe));
		}
		if(keyValues.isEmpty())
			return null;
		else{
            return (ExecIndexRow)scanDecoder.decode(keyValues);
		}
	}
	
	private ExecIndexRow getNextRowFromSource(boolean doClone) throws StandardException{
		ExecRow sourceRow;
		ExecIndexRow inputRow = null;
		if((sourceRow = source.getNextRowCore())!=null){
			rowsInput++;
			sourceExecIndexRow.execRowToExecIndexRow(doClone? sourceRow.getClone():sourceRow);
			inputRow = sourceExecIndexRow;
		}
		return inputRow;
	}
	
	@Override
	public ExecRow getExecRowDefinition() throws StandardException {
		SpliceLogUtils.trace(LOG,"getExecRowDefinition");
		ExecRow row = sourceExecIndexRow.getClone();
        SpliceUtils.populateDefaultValues(row.getRowArray(),0);
        return row;
	}

	protected void initializeScalarAggregation(ExecRow aggResult) throws StandardException{
		for(SpliceGenericAggregator aggregator: aggregates){
			aggregator.initialize(aggResult);
			aggregator.accumulate(aggResult,aggResult);
		}
	}
	
	protected void accumulateScalarAggregation(ExecRow nextRow,
									ExecRow aggResult,
									boolean hasDistinctAggregates,boolean isScan) throws StandardException{
		int size = aggregates.length;
		for(int i=0;i<size;i++){
			SpliceGenericAggregator currAggregate = aggregates[i];
			if(isScan||hasDistinctAggregates && !currAggregate.isDistinct()){
				currAggregate.merge(nextRow, aggResult);
			}else{
				currAggregate.accumulate(nextRow,aggResult);
			}
		}
	}

    @Override
    public RowEncoder getRowEncoder() throws StandardException {
        return RowEncoder.create(sourceExecIndexRow.nColumns(),null,null,DerbyBytesUtil.generateBytes(sequence[0]), KeyType.PREFIX_UNIQUE_POSTFIX_ONLY, RowType.COLUMNAR);
    }

    @Override
	public String toString() {
		return "ScalarAggregateOperation {source=" + source + "}";
	}
	
	public boolean isSingleInputRow() {
		return this.singleInputRow;
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
    public String prettyPrint(int indentLevel) {
        return "Scalar"+super.prettyPrint(indentLevel);
    }

}