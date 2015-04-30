package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.distinctscalar.DistinctAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.distinctscalar.DistinctScalarAggregateIterator;
import com.splicemachine.derby.impl.sql.execute.operations.distinctscalar.SingleDistinctScalarAggregateIterator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.*;
import com.splicemachine.derby.impl.storage.SpliceResultScanner;
import com.splicemachine.derby.stream.function.KeyerFunction;
import com.splicemachine.derby.stream.function.MergeAllAggregatesFunction;
import com.splicemachine.derby.stream.function.MergeNonDistinctAggregatesFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * The Distinct Scalar Aggregate is a three step process.  The steps occur as a CombinedRowProvider (first 2 steps) and then 
 * the reduce scan combines the last results.
 *
 *
 *
 * Step 1 Reading from Source and Writing to temp buckets with extraUniqueSequenceID prefix (Not needed in the case that data is sorted)
 *      If Distinct Keys Match
 * 				Merge Non Distinct Aggregates
 *      Else
 *      	add to buffer
 * 		Write to temp buckets
 *
 * Sorted
 *
 * Step2: Shuffle Intermediate Results to temp with uniqueSequenceID prefix
 *
 * 		If Keys Match
 * 			Merge Non Distinct Aggregates
 * 		else
 * 			Merge Distinct and Non Distinct Aggregates
 * 		Write to temp buckets
 *
 * Step 3: Combine N outputs
 * 		Merge Distinct and Non Distinct Aggregates
 * 		Flow through output of stack
 *
 * @author Scott Fines
 * Created on: 5/21/13
 */
public class DistinctScalarAggregateOperation extends GenericAggregateOperation {
    private static final long serialVersionUID = 1l;
    private byte[] extraUniqueSequenceID;
    private boolean isInSortedOrder;
    private int orderItem;
    private int[] keyColumns;
    private static final Logger LOG = Logger.getLogger(DistinctScalarAggregateOperation.class);
    private byte[] currentKey;
    private Scan baseScan;
    private DistinctScalarAggregateIterator step1Aggregator;
    private DistinctScalarAggregateIterator step2Aggregator;
    private SingleDistinctScalarAggregateIterator step3Aggregator;
    private boolean step3Closed;
    private DistinctAggregateBuffer buffer;
    private SpliceResultScanner scanner;

    protected static final String NAME = DistinctScalarAggregateOperation.class.getSimpleName().replaceAll("Operation", "");

    @Override
    public String getName() {
        return NAME;
    }

    @SuppressWarnings("UnusedDeclaration")
    public DistinctScalarAggregateOperation() {
    }

    @SuppressWarnings("UnusedParameters")
    public DistinctScalarAggregateOperation(SpliceOperation source,
                                            boolean isInSortedOrder,
                                            int aggregateItem,
                                            int orderItem,
                                            GeneratedMethod rowAllocator,
                                            int maxRowSize,
                                            int resultSetNumber,
                                            boolean singleInputRow,
                                            double optimizerEstimatedRowCount,
                                            double optimizerEstimatedCost) throws StandardException {
        super(source, aggregateItem, source.getActivation(), rowAllocator, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.orderItem = orderItem;
        this.isInSortedOrder = false; // XXX TODO Jleach: Optimize when data is already sorted.
        try {
            init(SpliceOperationContext.newContext(source.getActivation()));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        ExecRow clone = sourceExecIndexRow.getClone();
        // Set the default values to 0 in case a ProjectRestrictOperation has set the default values to 1.
        // That is done to avoid division by zero exceptions when executing a projection for defining the rows
        // before execution.
        SpliceUtils.populateDefaultValues(clone.getRowArray(), 0);
        return clone;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(isInSortedOrder);
        out.writeInt(orderItem);
        out.writeInt(extraUniqueSequenceID.length);
        out.write(extraUniqueSequenceID);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        isInSortedOrder = in.readBoolean();
        orderItem = in.readInt();
        extraUniqueSequenceID = new byte[in.readInt()];
        in.readFully(extraUniqueSequenceID);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        ExecPreparedStatement gsps = activation.getPreparedStatement();
        ColumnOrdering[] order =
                (ColumnOrdering[])
                        ((FormatableArrayHolder) gsps.getSavedObject(orderItem)).getArray(ColumnOrdering.class);
        keyColumns = new int[order.length];
        for (int index = 0; index < order.length; index++) {
            keyColumns[index] = order[index].getColumnId();
        }
        baseScan = context.getScan();
    }

    @Override
    public String toString() {
        return String.format("DistinctScalarAggregateOperation {resultSetNumber=%d, source=%s}", resultSetNumber, source);
    }

    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        DataSet<LocatedRow> dataSet = source.getDataSet();
        OperationContext operationContext = dsp.createOperationContext(this);
        LocatedRow finalRow = (LocatedRow) dataSet.keyBy(new KeyerFunction(operationContext, keyColumns))
                .reduceByKey(new MergeNonDistinctAggregatesFunction(operationContext, aggregates)).values()
                .fold(null, new MergeAllAggregatesFunction(operationContext, aggregates));
        return dsp.singleRowDataSet(finish(finalRow != null ? finalRow : new LocatedRow(new EmptyRowSupplier(aggregateContext).get()), aggregates));
    }

    public LocatedRow finish (LocatedRow locatedRow, SpliceGenericAggregator[] aggregates) throws StandardException {
        for(SpliceGenericAggregator aggregator:aggregates) {
            aggregator.finish(locatedRow.getRow());
        }
        return locatedRow;
    }

}