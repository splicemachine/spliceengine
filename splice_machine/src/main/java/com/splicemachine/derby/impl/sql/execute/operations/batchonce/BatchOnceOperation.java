/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations.batchonce;

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.FormatableIntHolder;
import org.spark_project.guava.base.Strings;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.function.BatchOnceFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

/**
 * Replaces a ProjectRestrictNode (below an Update) that would otherwise invoke a subquery for each source row.  This
 * operation will read a batch of rows from the source, transform them into the shape expected by update [old value, new
 * value, row location], execute the subquery once, and populate new value column of matching rows with the result.
 *
 * A complication is that we are using this operations where the subquery is expected to return at most one row for each
 * source row and must continue to enforce this, throwing LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION if more than one
 * subquery row is returned for each source row.
 *
 * Example Query: update A set A.name = (select B.name from B where A.id = B.id) where A.name is null;
 *
 * Currently we ONLY support BatchOnce for queries that have a single subquery and where that subquery has a single
 * equality predicate with one correlated column reference.
 *
 * Currently BatchOnce is only used in the case where the subquery tree has a FromBaseTable leaf, no index. So with
 * or without BatchOnce we always scan the entire subquery table.  With BatchOnce however the number of times
 * we scan the entire subquery tables is potentially divided by this class's configurable BATCH_SIZE.  This
 * can make a 24+hour query execute in a few tens of seconds.
 *
 * Related: BatchOnceNode, BatchOnceVisitor, OnceOperation
 */
public class BatchOnceOperation extends SpliceBaseOperation {

    private static final Logger LOG = Logger.getLogger(BatchOnceOperation.class);


    private SpliceOperation source;
    private SpliceOperation subquerySource;
    /* Name of the activation field that contains the Update result set. Used to get currentRowLocation */
    private String updateResultSetFieldName;

    /* 1-based column positions for columns in the source we will need.  Will be -1 if not available. */
    private int[] sourceCorrelatedColumnPositions;
    private int[] subqueryCorrelatedColumnPositions;

    private int sourceCorrelatedColumnItem;
    private int subqueryCorrelatedColumnItem;

    public BatchOnceOperation() {
    }

    public BatchOnceOperation(SpliceOperation source,
                              Activation activation,
                              int rsNumber,
                              SpliceOperation subquerySource,
                              String updateResultSetFieldName,
                              int sourceCorrelatedColumnItem,
                              int subqueryCorrelatedColumnItem) throws StandardException {
        super(activation, rsNumber, 0d, 0d);
        this.source = source;
        this.subquerySource = subquerySource.getLeftOperation();
        this.updateResultSetFieldName = updateResultSetFieldName;
        this.sourceCorrelatedColumnItem = sourceCorrelatedColumnItem;
        this.subqueryCorrelatedColumnItem = subqueryCorrelatedColumnItem;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        source.init(context);
        subquerySource.init(context);
        sourceCorrelatedColumnPositions = generateColumnPositions(sourceCorrelatedColumnItem);
        subqueryCorrelatedColumnPositions = generateColumnPositions(subqueryCorrelatedColumnItem);
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

       @Override
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList(source);
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
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);
        return "BatchOnceOperation:" + indent
                + "resultSetNumber:" + resultSetNumber + indent
                + "source:" + source.prettyPrint(indentLevel + 1);
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return source;
    }

    @Override
    public void open() throws StandardException {
        super.open();
        if (source != null) {
            source.open();
        }
    }

    @Override
    public void close() throws StandardException {
        super.close();
        source.close();
    }

    // - - - - - - - - - - - - - - - - - - -
    // serialization
    // - - - - - - - - - - - - - - - - - - -

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.source = (SpliceOperation) in.readObject();
        this.subquerySource = (SpliceOperation) in.readObject();
        this.updateResultSetFieldName = in.readUTF();
        this.sourceCorrelatedColumnPositions = ArrayUtil.readIntArray(in);
        this.subqueryCorrelatedColumnPositions = ArrayUtil.readIntArray(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.source);
        out.writeObject(this.subquerySource);
        out.writeUTF(this.updateResultSetFieldName);
        ArrayUtil.writeIntArray(out, this.sourceCorrelatedColumnPositions);
        ArrayUtil.writeIntArray(out, this.subqueryCorrelatedColumnPositions);
    }

    public SpliceOperation getSubquerySource() {
        return subquerySource;
    }

    public int[] getSourceCorrelatedColumnPositions() {
        return sourceCorrelatedColumnPositions;
    }

    public int[] getSubqueryCorrelatedColumnPositions() {
        return subqueryCorrelatedColumnPositions;
    }

    public SpliceOperation getSource() {
        return source;
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        DataSet set = source.getDataSet(dsp);
        OperationContext<BatchOnceOperation> operationContext = dsp.createOperationContext(this);
        return set.mapPartitions(new BatchOnceFunction(operationContext));
    }

    protected int[] generateColumnPositions(int columnItem) {
        FormatableIntHolder[] fihArray = (FormatableIntHolder[]) activation.getPreparedStatement().getSavedObject(columnItem);
        int[] cols = new int[fihArray.length];
        for (int i = 0, s = fihArray.length; i < s; i++){
            cols[i] = fihArray[i].getInt();
        }
        return cols;
    }
}
