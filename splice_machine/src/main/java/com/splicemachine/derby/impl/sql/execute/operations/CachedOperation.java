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

package com.splicemachine.derby.impl.sql.execute.operations;

import org.spark_project.guava.base.Strings;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.pipeline.Exceptions;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Operation for holding in-memory result set
 *
 * @author P Trolard
 *         Date: 04/02/2014
 */
public class CachedOperation extends SpliceBaseOperation {

    private static Logger LOG = Logger.getLogger(CachedOperation.class);
    protected static final String NAME = CachedOperation.class.getSimpleName().replaceAll("Operation", "");
    private SpliceOperation source;
    private DataSet ds;
    private boolean populated;
    private List<ExecRow> rows;

    @Override
    public String getName() {
        return NAME;
    }


    public CachedOperation() {
    }

    public CachedOperation(Activation activation, SpliceOperation source, int resultSetNumber) throws StandardException {
        super(activation, resultSetNumber, 0, 0);
        this.source = source;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        try {
            super.init(context);
            source.init(context);
            if (!populated) {
                populateCache();
                populated = true;
            }
        }
        catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return source.getExecRowDefinition();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation)in.readObject();
        populated = in.readBoolean();
        rows = (List)in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(source);

        try {
            if (!populated) {
                populated = true;
                populateCache();
            }
        } catch (StandardException e) {
            throw new IOException(e);
        }
        out.writeBoolean(populated);
        out.writeObject(rows);
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList(source);
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return new int[0];
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return false;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n" + Strings.repeat("\t", indentLevel);
        return new StringBuilder("CachedOp")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("rowsCached:").append(rows.size())
                .append(indent).append("first 10:").append(rows.subList(0, 10))
                .toString();
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {

        if (rows.size() > 0) {
            DataSet dataSet = dsp.createDataSet(rows.iterator());
            ds = dataSet.map(new CacheFunction(dsp.createOperationContext(this)));
        }

        if (ds != null) {
            return ds;
        }
        else {
            return source.getDataSet(dsp);
        }
    }

    public static class CacheFunction extends SpliceFunction<CachedOperation, ExecRow, LocatedRow> {

        public CacheFunction() {

        }

        public CacheFunction(OperationContext<CachedOperation> operationContext) {
            super(operationContext);
        }

        @Override
        public LocatedRow call(ExecRow execRow) throws Exception {
            getOperation().source.setCurrentRow(execRow);
            return new LocatedRow(execRow);
        }
    }

    private void populateCache() throws StandardException {

        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        int maxMemoryPerTable = lcc.getOptimizerFactory().getMaxMemoryPerTable();
        if(maxMemoryPerTable<=0)
            return;

        source.openCore();
        rows = new LinkedList<>();
        ExecRow aRow;
        int cacheSize = 0;
        FormatableBitSet toClone = null;

        aRow = source.getNextRowCore();
        if (aRow != null)
        {
            toClone = new FormatableBitSet(aRow.nColumns() + 1);
            toClone.set(1);
        }
        while (aRow != null)
        {
            cacheSize += aRow.getColumn(1).getLength();
            if (cacheSize > maxMemoryPerTable ||
                    rows.size() > Optimizer.MAX_DYNAMIC_MATERIALIZED_ROWS) {
                rows.clear();
                break;
            }
            rows.add(aRow.getClone(toClone));
            aRow = source.getNextRowCore();
        }
        source.close();
    }
}