/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.NLJAntiJoinFunction;
import com.splicemachine.derby.stream.function.NLJInnerJoinFunction;
import com.splicemachine.derby.stream.function.NLJOneRowInnerJoinFunction;
import com.splicemachine.derby.stream.function.NLJOuterJoinFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class NestedLoopJoinOperation extends JoinOperation {
		private static Logger LOG = Logger.getLogger(NestedLoopJoinOperation.class);
		protected boolean isHash;
        protected static final String NAME = NestedLoopJoinOperation.class.getSimpleName().replaceAll("Operation","");
    	@Override
    	public String getName() {
    			return NAME;
    	}
		public NestedLoopJoinOperation() {
				super();
		}

		public NestedLoopJoinOperation(SpliceOperation leftResultSet,
																	 int leftNumCols,
																	 SpliceOperation rightResultSet,
																	 int rightNumCols,
																	 Activation activation,
																	 GeneratedMethod restriction,
																	 int resultSetNumber,
																	 boolean oneRowRightSide,
																	 boolean notExistsRightSide,
									                                 boolean rightFromSSQ,
																	 double optimizerEstimatedRowCount,
																	 double optimizerEstimatedCost,
																	 String userSuppliedOptimizerOverrides,
																	 String sparkExpressionTreeAsString) throws StandardException {
				super(leftResultSet,leftNumCols,rightResultSet,rightNumCols,activation,restriction,
								resultSetNumber,oneRowRightSide,notExistsRightSide,rightFromSSQ,optimizerEstimatedRowCount,
								optimizerEstimatedCost,userSuppliedOptimizerOverrides,sparkExpressionTreeAsString);
				this.isHash = false;
                init();
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
				super.readExternal(in);
				isHash = in.readBoolean();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeBoolean(isHash);
		}

		@Override
		public void init(SpliceOperationContext context) throws IOException, StandardException{
				super.init(context);
		}

		@Override
		public String toString() {
				return "NestedLoop"+super.toString();
		}

		@Override
		public String prettyPrint(int indentLevel) {
				return "NestedLoopJoin:" + super.prettyPrint(indentLevel);
		}


    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
		if (!isOpen)
			throw new IllegalStateException("Operation is not open");

		DataSet<ExecRow> left = leftResultSet.getDataSet(dsp);
        OperationContext<NestedLoopJoinOperation> operationContext = dsp.createOperationContext(this);

        operationContext.pushScope();
        try {
            if (isOuterJoin())
                return left.mapPartitions(new NLJOuterJoinFunction(operationContext), true);
            else {
                if (notExistsRightSide)
					return left.mapPartitions(new NLJAntiJoinFunction(operationContext), true);
				else {
					if (oneRowRightSide)
						return left.mapPartitions(new NLJOneRowInnerJoinFunction(operationContext), true);
					else
						return left.mapPartitions(new NLJInnerJoinFunction(operationContext), true);
				}

			}
        } finally {
            operationContext.popScope();
        }
    }
}
