/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.spark_project.guava.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;

/**
 * Takes a quantified predicate subquery's result set.
 * NOTE: A row with a single column containing null will be returned from
 * getNextRow() if the underlying subquery ResultSet is empty.
 *
 */
public class AnyOperation extends SpliceBaseOperation {
	private static Logger LOG = Logger.getLogger(AnyOperation.class);
    protected static final String NAME = AnyOperation.class.getSimpleName().replaceAll("Operation","");
	@Override
	public String getName() {
			return NAME;
	}
    
	/* Used to cache row with nulls for case when subquery result set
	 * is empty.
	 */
	private ExecRow rowWithNulls;

	/* Used to cache the StatementContext */
	private StatementContext statementContext;

    // set in constructor and not altered during
    // life of object.
    public SpliceOperation source;
	private SpliceMethod<ExecRow> emptyRowFun;
    private String emptyRowFunName;

	public int subqueryNumber;
	public int pointOfAttachment;

    //
    // class interface
    //


    public AnyOperation() { }

		public AnyOperation(SpliceOperation s, Activation a, GeneratedMethod emptyRowFun,
												int resultSetNumber, int subqueryNumber,
												int pointOfAttachment,
												double optimizerEstimatedRowCount,
												double optimizerEstimatedCost) throws StandardException {
				super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
				source = s;
				this.subqueryNumber = subqueryNumber;
				this.pointOfAttachment = pointOfAttachment;
				this.emptyRowFunName = emptyRowFun.getMethodName();
                init();
		}

    @Override
    public SpliceOperation getLeftOperation() {
        return source;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(source);
        out.writeUTF(emptyRowFunName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation) in.readObject();
        emptyRowFunName = in.readUTF();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        source.init(context);
        if(emptyRowFun==null)
            emptyRowFun = new SpliceMethod<ExecRow>(emptyRowFunName,activation);
    }

    private ExecRow getRowWithNulls() throws StandardException {
        if (rowWithNulls == null){
            rowWithNulls = emptyRowFun.invoke();
        }
        return rowWithNulls;
    }
    @Override
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList(source);
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);
        return "Any:" +
                indent + "resultSetNumber:" + resultSetNumber +
                indent + "Source:" + source.prettyPrint(indentLevel + 1) +
                indent + "emptyRowFunName:" + emptyRowFunName +
                indent + "subqueryNumber:" + subqueryNumber;
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
    public String toString() {
        return String.format("AnyOperation {source=%s,resultSetNumber=%d}",source,resultSetNumber);
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        // we are consuming the dataset, get a ResultDataSet
        Iterator<LocatedRow> iterator = source.getResultDataSet(dsp).toLocalIterator();
        if (iterator.hasNext())
                return dsp.singleRowDataSet(new LocatedRow(iterator.next().getRow()));
        return dsp.singleRowDataSet(new LocatedRow(getRowWithNulls()));
    }

}