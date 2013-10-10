package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.storage.ScalarAggregateRowProvider;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.SQLWarningFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
/**
 * Operation for performing Scalar Aggregations (sum, avg, max/min, etc.). 
 *  
 * @author Scott Fines
 * 
 * 
 *
 */
public class ScalarAggregateOperation extends GenericAggregateOperation {
	public static long serialVersionUID = 1l;
	private static Logger LOG = Logger.getLogger(ScalarAggregateOperation.class);
	
	protected boolean isInSortedOrder;
	protected boolean singleInputRow;

	protected boolean isOpen=false;

    private ScalarAggregator scanAggregator;
    private ScalarAggregator sinkAggregator;

    @SuppressWarnings("UnusedDeclaration")
    public ScalarAggregateOperation () {
		super();
	}

    public ScalarAggregateOperation(SpliceOperation s,
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
	public void open() throws StandardException, IOException {
        super.open();
		source.open();
		isOpen=true;
	}
	
	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,RowDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
		try {
            reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID, SpliceUtils.NA_TRANSACTION_ID);
            //make sure that we filter out failed tasks
            SuccessFilter filter = new SuccessFilter(failedTasks);
            reduceScan.setFilter(filter);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        SpliceUtils.setInstructions(reduceScan,activation,top,spliceRuntimeContext);
        return new ScalarAggregateRowProvider("scalarAggregateReduce",SpliceOperationCoprocessor.TEMP_TABLE,
                reduceScan,rowDecoder, spliceRuntimeContext,aggregates);
	}

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
    	return getReduceRowProvider(top, rowDecoder, spliceRuntimeContext);
    }

    @Override
	public void init(SpliceOperationContext context) throws StandardException{
		super.init(context);
		ExecutionFactory factory = operationInformation.getExecutionFactory();
		try {
			sortTemplateRow = factory.getIndexableRow(rowAllocator.invoke());
			sourceExecIndexRow = factory.getIndexableRow(sortTemplateRow);
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
	}

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        if(reduceScan!=null)
            SpliceDriver.driver().getTempCleaner().deleteRange(uniqueSequenceID,reduceScan.getStartRow(),reduceScan.getStopRow());
    }



    @Override
    protected JobStats doShuffle() throws StandardException {
        long start = System.currentTimeMillis();
        SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
        final RowProvider rowProvider = source.getMapRowProvider(this, getRowEncoder(spliceRuntimeContext).getDual(getExecRowDefinition()), spliceRuntimeContext);
        nextTime+= System.currentTimeMillis()-start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,spliceRuntimeContext);
        return rowProvider.shuffleRows(soi);
    }
    
    @Override
	public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(scanAggregator==null){
            RowDecoder decoder = getRowEncoder(spliceRuntimeContext).getDual(getExecRowDefinition());
            scanAggregator = new ScalarAggregator(new ScalarAggregateScan(decoder,regionScanner),
                    aggregates,true,false);
        }

        ExecRow aggregate = scanAggregator.aggregate(spliceRuntimeContext);
        if(aggregate!=null)
            return finish(aggregate, scanAggregator);
        return null;
	}

    @Override
    public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(sinkAggregator==null){
            sinkAggregator = new ScalarAggregator(new OperationScalarAggregateSource(source,sourceExecIndexRow,false),aggregates,false,true);
        }

        ExecRow aggregate = sinkAggregator.aggregate(spliceRuntimeContext);
        if(aggregate!=null)
            return finish(aggregate,sinkAggregator);
        return null;
    }


    private ExecRow finish(ExecRow row,ScalarAggregator aggregator) throws StandardException {
        SpliceLogUtils.trace(LOG, "finishAggregation");

		/*
		** If the row in which we are to place the aggregate
		** result is null, then we have an empty input set.
		** So we'll have to create our own row and set it
		** up.  Note: we needn't initialize in this case,
		** finish() will take care of it for us.
		*/
        if (row == null) {
            row = this.getActivation().getExecutionFactory().getIndexableRow(rowAllocator.invoke());
        }
        setCurrentRow(row);
        boolean eliminatedNulls = aggregator.finish(row);

        if (eliminatedNulls)
            addWarning(SQLWarningFactory.newSQLWarning(SQLState.LANG_NULL_ELIMINATED_IN_SET_FUNCTION));

        return row;
    }

	@Override
	public ExecRow getExecRowDefinition() throws StandardException {
		if (LOG.isTraceEnabled())
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
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG,"accumulateScalarAggregation");
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
    public RowEncoder getRowEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return RowEncoder.create(sourceExecIndexRow.nColumns(),null,null,
                uniqueSequenceID,
                KeyType.PREFIX_UNIQUE_POSTFIX_ONLY,
                RowMarshaller.packed());
    }

    @Override
	public String toString() {
		return "ScalarAggregateOperation {source=" + source + "}";
	}
	
	public boolean isSingleInputRow() {
		return this.singleInputRow;
	}
	
    @Override
    public String prettyPrint(int indentLevel) {
        return "Scalar"+super.prettyPrint(indentLevel);
    }

}