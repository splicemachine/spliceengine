/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.function.CloneFunction;
import com.splicemachine.derby.stream.function.OffsetFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import splice.com.google.common.base.Strings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
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
    private boolean bypass = false; // set to true if we implement the limit/offset on the streaming connection


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


    public long getTotalOffset() throws StandardException {
        if (offsetMethod != null) {
            DataValueDescriptor offVal = offsetMethod.invoke();
            if (offVal.isNotNull().getBoolean()) {
                offset = offVal.getLong();
            }
        }
        return offset;
    }

    public long getFetchLimit() throws StandardException {
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

    public void setBypass() {
        bypass = true;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation) in.readObject();
        offsetMethodName = readNullableString(in);
        fetchFirstMethodName = readNullableString(in);
        hasJDBCLimitClause = in.readBoolean();
        bypass = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(source);
        writeNullableString(offsetMethodName, out);
        writeNullableString(fetchFirstMethodName, out);
        out.writeBoolean(hasJDBCLimitClause);
        out.writeBoolean(bypass);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        if (bypass) {
            return source.getDataSet(dsp);
        }
        final long fetchLimit = getFetchLimit();
        long offset = getTotalOffset();
        OperationContext operationContext = dsp.createOperationContext(this);
        dsp.incrementOpDepth();
        DataSet<ExecRow> sourceDS = source.getDataSet(dsp);
        dsp.decrementOpDepth();
        DataSet<ExecRow> sourceSet = sourceDS.map(new CloneFunction<>(operationContext));
        if (dsp.isSparkExplain()) {
            DataSet<ExecRow> ds = dsp.getEmpty();
            handleSparkExplain(ds, sourceDS, dsp);
            return ds;
        }
        else
            return sourceSet.zipWithIndex(operationContext).mapPartitions(new OffsetFunction<SpliceOperation, ExecRow>(operationContext, offset, fetchLimit));
    }

    @Override
    public String getScopeName() {
        return "Row Limit";
    }

}
