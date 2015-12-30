package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.AggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.OperationInformation;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.spark.SparkConstants;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

public class GroupedAggregateOperation extends GenericAggregateOperation {
    private static final long serialVersionUID = 1l;
    private static Logger LOG = Logger.getLogger(GroupedAggregateOperation.class);
    protected boolean isInSortedOrder;
    protected boolean isRollup;
    public GroupedAggregateContext groupedAggregateContext;
    private boolean[] usedTempBuckets;
    protected static final String NAME = GroupedAggregateOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}


    public GroupedAggregateOperation() {
        super();
        SpliceLogUtils.trace(LOG, "instantiate without parameters");
    }

    public GroupedAggregateOperation(
                                        SpliceOperation source,
                                        OperationInformation baseOpInformation,
                                        AggregateContext genericAggregateContext,
                                        GroupedAggregateContext groupedAggregateContext,
                                        boolean isInSortedOrder,
                                        boolean isRollup) throws StandardException {
        super(source, baseOpInformation, genericAggregateContext);
        this.isRollup = isRollup;
        this.isInSortedOrder = isInSortedOrder;
        this.groupedAggregateContext = groupedAggregateContext;
    }

    @SuppressWarnings("UnusedParameters")
    public GroupedAggregateOperation(
                                        SpliceOperation s,
                                        boolean isInSortedOrder,
                                        int aggregateItem,
                                        Activation a,
                                        GeneratedMethod ra,
                                        int maxRowSize,
                                        int resultSetNumber,
                                        double optimizerEstimatedRowCount,
                                        double optimizerEstimatedCost,
                                        boolean isRollup,
                                        GroupedAggregateContext groupedAggregateContext) throws
                                                                                         StandardException {
        super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.isInSortedOrder = isInSortedOrder;
        this.isRollup = isRollup;
        this.groupedAggregateContext = groupedAggregateContext;
        recordConstructorTime();
    }

    public GroupedAggregateOperation(SpliceOperation s,
                                     boolean isInSortedOrder,
                                     int aggregateItem,
                                     int orderingItem,
                                     Activation a,
                                     GeneratedMethod ra,
                                     int maxRowSize,
                                     int resultSetNumber,
                                     double optimizerEstimatedRowCount,
                                     double optimizerEstimatedCost,
                                     boolean isRollup) throws StandardException {
        this(s, isInSortedOrder, aggregateItem, a, ra, maxRowSize, resultSetNumber,
                optimizerEstimatedRowCount, optimizerEstimatedCost, isRollup, new DerbyGroupedAggregateContext(orderingItem));
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
                                                    ClassNotFoundException {
        super.readExternal(in);
        isInSortedOrder = in.readBoolean();
        isRollup = in.readBoolean();
        groupedAggregateContext = (GroupedAggregateContext) in.readObject();
        if(in.readBoolean())
            usedTempBuckets = ArrayUtil.readBooleanArray(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(isInSortedOrder);
        out.writeBoolean(isRollup);
        out.writeObject(groupedAggregateContext);
        out.writeBoolean(usedTempBuckets!=null);
        if(usedTempBuckets!=null){
            ArrayUtil.writeBooleanArray(out,usedTempBuckets);
        }
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException,
                                                            IOException {
        SpliceLogUtils.trace(LOG, "init called");
        super.init(context);
        source.init(context);
        groupedAggregateContext.init(context, aggregateContext);
    }

    @Override
    public ExecRow getExecRowDefinition() {
        SpliceLogUtils.trace(LOG, "getExecRowDefinition");
        return sourceExecIndexRow.getClone();
    }

    @Override
    public String toString() {
        return String.format("GroupedAggregateOperation {resultSetNumber=%d, source=%s}", resultSetNumber, source);
    }


    public boolean isInSortedOrder() {
        return this.isInSortedOrder;
    }

    public boolean hasDistinctAggregate() {
        return groupedAggregateContext.getNumDistinctAggregates() > 0;
    }

    public Properties getSortProperties() {
        Properties sortProperties = new Properties();
        sortProperties.setProperty("numRowsInput", "" + getRowsInput());
        sortProperties.setProperty("numRowsOutput", "" + getRowsOutput());
        return sortProperties;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "Grouped" + super.prettyPrint(indentLevel);
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        OperationContext<GroupedAggregateOperation> operationContext = dsp.createOperationContext(this);
        
        DataSet set = source.getDataSet(dsp);

        operationContext.pushScope();
        set = set.map(new CountReadFunction(operationContext));
        operationContext.popScope();

        if (groupedAggregateContext.getNonGroupedUniqueColumns() != null &&
            groupedAggregateContext.getNonGroupedUniqueColumns().length > 0) {
            // Distinct Aggregate Path
            int[] allKeys = ArrayUtils.addAll(groupedAggregateContext.getGroupingKeys(), groupedAggregateContext.getNonGroupedUniqueColumns());

            operationContext.pushScopeForOp(SparkConstants.SCOPE_GROUP_AGGREGATE_KEYER);
            PairDataSet set2 = set.keyBy(new KeyerFunction(operationContext, allKeys));
            operationContext.popScope();
            
            operationContext.pushScopeForOp("Reduce");
            PairDataSet set3 = set2.reduceByKey(new MergeNonDistinctAggregatesFunction(operationContext));
            operationContext.popScope();
            
            operationContext.pushScopeForOp("Read Values");
            DataSet set4 = set3.values();
            operationContext.popScope();
            
            set = set4;
        }
        
        if (isRollup) { // OLAP Rollup Functionality
            operationContext.pushScopeForOp("Rollup");
            set = set.flatMap(new GroupedAggregateRollupFlatMapFunction(operationContext));
            operationContext.popScope();
        }
        
        operationContext.pushScopeForOp(SparkConstants.SCOPE_GROUP_AGGREGATE_KEYER);
        PairDataSet set2 = set.keyBy(new KeyerFunction(operationContext, groupedAggregateContext.getGroupingKeys()));
        operationContext.popScope();
        
        operationContext.pushScopeForOp("Reduce");
        PairDataSet set3 = set2.reduceByKey(new MergeAllAggregatesFunction(operationContext));
        operationContext.popScope();
        
        operationContext.pushScopeForOp("Read Values");
        DataSet set4 = set3.values("Read Values");
        operationContext.popScope();
        
        operationContext.pushScopeForOp("Finalize");
        DataSet set5 = set4.map(new AggregateFinisherFunction(operationContext), true);
        operationContext.popScope();
        
        return set5;
    }

}
