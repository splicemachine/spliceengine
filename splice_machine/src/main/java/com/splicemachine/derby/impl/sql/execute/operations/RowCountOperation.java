package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.function.TakeFunction;
import org.sparkproject.guava.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.function.OffsetFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.sparkproject.guava.collect.Iterators;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * RowCountOperation is used for the following types of queries:
 *
 * SELECT * FROM T FETCH FIRST ROW ONLY
 * SELECT * FROM T ORDER BY I OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY
 * SELECT * FROM T ORDER BY I OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY
 * SELECT * FROM T OFFSET 100 ROWS
 * SELECT * FROM T { LIMIT 10 }
 * SELECT TOP N * FROM T
 *
 * @author Scott Fines
 *         Created on: 5/15/13
 */
public class RowCountOperation extends SpliceBaseOperation {
    private static final long serialVersionUID = 1l;
    /* When the reduce scan is sequential this operation adds this column to the results to indicate
     * how many rows have been skipped in the current region. */
    private String offsetMethodName;
    private String fetchFirstMethodName;
    private SpliceMethod<DataValueDescriptor> offsetMethod;
    private SpliceMethod<DataValueDescriptor> fetchFirstMethod;
    private boolean hasJDBCLimitClause;
    private SpliceOperation source;
    private long offset;
    private long fetchLimit;


    protected static final String NAME = RowCountOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

    
    public RowCountOperation() {
    }

    public RowCountOperation(SpliceOperation source,
                             Activation activation,
                             int resultSetNumber,
                             GeneratedMethod offsetMethod,
                             GeneratedMethod fetchFirstMethod,
                             boolean hasJDBCLimitClause,
                             double optimizerEstimatedRowCount,
                             double optimizerEstimatedCost) throws StandardException {
        super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.offsetMethodName = (offsetMethod == null) ? null : offsetMethod.getMethodName();
        this.fetchFirstMethodName = (fetchFirstMethod == null) ? null : fetchFirstMethod.getMethodName();
        this.hasJDBCLimitClause = hasJDBCLimitClause;
        this.source = source;
        init();
        offset = getTotalOffset();
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.singletonList(source);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        source.init(context);
        if (offsetMethodName != null) {
            offsetMethod = new SpliceMethod<>(offsetMethodName, activation);
        }
        if (fetchFirstMethodName != null) {
            fetchFirstMethod = new SpliceMethod<>(fetchFirstMethodName, activation);
        }
    }


    private long getTotalOffset() throws StandardException {
        if (offsetMethod != null) {
            DataValueDescriptor offVal = offsetMethod.invoke();
            if (offVal.isNotNull().getBoolean()) {
                offset = offVal.getLong();
            }
        }
        return offset;
    }

    private long getFetchLimit() throws StandardException {
        if (fetchFirstMethod != null) {
            DataValueDescriptor fetchFirstVal = fetchFirstMethod.invoke();
            if (fetchFirstVal.isNotNull().getBoolean()) {
                fetchLimit = fetchFirstVal.getLong();
            }
        }
        return fetchLimit;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n" + Strings.repeat("\t", indentLevel);

        return "RowCount:" + indent + "resultSetNumber:" + resultSetNumber
                + indent + "offsetMethodName:" + offsetMethodName
                + indent + "fetchFirstMethodName:" + fetchFirstMethodName
                + indent + "source:" + source.prettyPrint(indentLevel + 1);
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return source.getExecRowDefinition();
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return source;
    }

    public SpliceOperation getSource() {
        return source;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation) in.readObject();
        offsetMethodName = readNullableString(in);
        fetchFirstMethodName = readNullableString(in);
        hasJDBCLimitClause = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(source);
        writeNullableString(offsetMethodName, out);
        writeNullableString(fetchFirstMethodName, out);
        out.writeBoolean(hasJDBCLimitClause);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        final long fetchLimit = getFetchLimit();
        long offset = getTotalOffset();
        OperationContext operationContext = dsp.createOperationContext(this);
        DataSet<LocatedRow> sourceSet = source.getDataSet(dsp);
        return sourceSet.zipWithIndex().mapPartitions(new OffsetFunction<SpliceOperation, LocatedRow>(operationContext, offset, fetchLimit));
    }

    @Override
    public DataSet<LocatedRow> getResultDataSet(DataSetProcessor dsp) throws StandardException {
        final long fetchLimit = getFetchLimit();
        long offset = getTotalOffset();
        if (offset > 0) {
            // no optimization, return getDataSet()
            return getDataSet(dsp);
        }
        DataSet<LocatedRow> sourceSet = source.getDataSet(dsp);
        if (fetchLimit > 0) {
            OperationContext operationContext = dsp.createOperationContext(this);
            // Apply limit on control side
            sourceSet = sourceSet.mapPartitions(new TakeFunction<SpliceOperation, LocatedRow>(operationContext, (int) fetchLimit));
            final DataSet<LocatedRow> finalSourceSet = sourceSet;
            sourceSet = new ControlDataSet<>(new Iterable<LocatedRow>() {
                @Override
                public Iterator<LocatedRow> iterator() {
                    return  Iterators.limit(finalSourceSet.toLocalIterator(), (int) fetchLimit);
                }
            });
        }
        return sourceSet;
    }
    
    @Override
    public String getScopeName() {
        return "Row Limit";
    }

}
