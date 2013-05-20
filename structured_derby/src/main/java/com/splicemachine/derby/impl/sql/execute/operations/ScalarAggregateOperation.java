package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.Accumulator;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.stats.TimingStats;
import com.splicemachine.derby.utils.*;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
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
    protected Accumulator scanAccumulator = TimingStats.uniformAccumulator();
    /*indicates whether or not this operation is looking at the TEMP space*/
    private boolean isTemp;

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
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow template) throws StandardException {
        try {
            reduceScan = Scans.buildPrefixRangeScan(sequence[0], getTransactionID());
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        SpliceUtils.setInstructions(reduceScan,activation,top);
        return new ClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,template,null);
	}

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        return ((SpliceOperation)source).getMapRowProvider(top,template);
    }

    @Override
	public void init(SpliceOperationContext context) throws StandardException{
		super.init(context);
		ExecutionFactory factory = activation.getExecutionFactory();
		try {
			sortTemplateRow = factory.getIndexableRow((ExecRow)rowAllocator.invoke(activation));
			sourceExecIndexRow = factory.getIndexableRow(sortTemplateRow);

            isTemp = !context.isSink()||context.getTopOperation()!=this;
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG,"getNextRowCore");
		return doAggregation(isTemp,scanAccumulator);
	}

	protected ExecRow doAggregation(boolean useScan,Accumulator stats) throws StandardException{
		ExecIndexRow execIndexRow;
		ExecIndexRow aggResult = null;
		if(useScan){
            do{
                long processTime = System.nanoTime();
                execIndexRow = getNextRowFromScan(false);
                if(execIndexRow==null)continue;

                SpliceLogUtils.trace(LOG,"aggResult =%s before",aggResult);
                aggResult = aggregate(execIndexRow,aggResult,false,true);
                SpliceLogUtils.trace(LOG,"aggResult =%s after",aggResult);

                stats.tick(System.nanoTime()-processTime);
            }while(execIndexRow!=null);
		}else{
            do{
                long processTime = System.nanoTime();
                execIndexRow = getNextRowFromSource(false);
                if(execIndexRow==null) continue;

                aggResult = aggregate(execIndexRow,aggResult,true,false);

                stats.tick(System.nanoTime()-processTime);
            }while(execIndexRow!=null);
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
									ExecIndexRow aggResult, boolean doInitialize, boolean isScan) throws StandardException{
		if(aggResult==null){
			aggResult = (ExecIndexRow)execIndexRow.getClone();
			SpliceLogUtils.trace(LOG, "aggResult = %s aggregate before",aggResult);
			if(doInitialize){
				initializeScalarAggregation(aggResult);
				SpliceLogUtils.trace(LOG, "aggResult = %s aggregate after",aggResult);
			}
		}else
			accumulateScalarAggregation(execIndexRow, aggResult, false,isScan);
		return aggResult;
	}
	
	protected ExecIndexRow getNextRowFromScan(boolean doClone) throws StandardException {
		//TODO -sf- make sure that only one scanner does the final aggregation 
		SpliceLogUtils.trace(LOG, "getting next result from TEMP Table");
		List<KeyValue> keyValues = new ArrayList<KeyValue>();
		try{
			regionScanner.next(keyValues);
			SpliceLogUtils.trace(LOG,"keyValues.length=%d, regionScanner=%s",keyValues.size(),regionScanner);
		}catch(IOException ioe){
			SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION, ioe));
		}
		if(keyValues.isEmpty())
			return null;
		else{
			SpliceLogUtils.trace(LOG,"populating next row");
			ExecIndexRow row = (ExecIndexRow)sourceExecIndexRow.getClone();
			SpliceUtils.populate(keyValues,row.getRowArray());
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
									boolean hasDistinctAggregates,boolean isScan) throws StandardException{
		int size = aggregates.length;
		for(int i=0;i<size;i++){
			SpliceGenericAggregator currAggregate = aggregates[i];
			if(isScan||hasDistinctAggregates &&
					!currAggregate.isDistinct()){
				currAggregate.merge(nextRow, aggResult);
			}else{
				currAggregate.accumulate(nextRow,aggResult);
			}
		}
	}

    @Override
    public OperationSink.Translator getTranslator() throws IOException {
        final Serializer serializer = new Serializer();

        return new OperationSink.Translator() {
            @Nonnull
            @Override
            public List<Mutation> translate(@Nonnull ExecRow row) throws IOException {
                try {
                    byte[] key = DerbyBytesUtil.generatePrefixedRowKey(sequence[0]);
                    Put put = Puts.buildTempTableInsert(key,row.getRowArray(),null,serializer);
                    return Collections.<Mutation>singletonList(put);
                } catch (StandardException e) {
                    throw Exceptions.getIOException(e);
                }
            }
        };
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