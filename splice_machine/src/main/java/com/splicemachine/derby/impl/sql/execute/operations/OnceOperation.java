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
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.shared.common.sanity.SanityManager;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OnceOperation extends SpliceBaseOperation {
		private static final long serialversionUID = 1l;
		private static Logger LOG = Logger.getLogger(OnceOperation.class);
		public static final int DO_CARDINALITY_CHECK		= 1;
		public static final int NO_CARDINALITY_CHECK		= 2;
		public static final int UNIQUE_CARDINALITY_CHECK	= 3;
		private ExecRow rowWithNulls;
		// set in constructor and not altered during
		// life of object.
		public SpliceOperation source;
		protected SpliceMethod<ExecRow> emptyRowFun;
		protected String emptyRowFunMethodName;
		private int cardinalityCheck;
		public int subqueryNumber;
		public int pointOfAttachment;

	    protected static final String NAME = OnceOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}
		
		@Deprecated
		public OnceOperation(){}

		public OnceOperation(SpliceOperation s, Activation a, GeneratedMethod emptyRowFun,
												 int cardinalityCheck, int resultSetNumber,
												 int subqueryNumber, int pointOfAttachment,
												 double optimizerEstimatedRowCount,
												 double optimizerEstimatedCost) throws StandardException {
				super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
				SpliceLogUtils.trace(LOG, "instantiated");
				this.source = s;
				this.emptyRowFunMethodName = (emptyRowFun != null)?emptyRowFun.getMethodName():null;
				this.cardinalityCheck = cardinalityCheck;
				this.subqueryNumber = subqueryNumber;
				this.pointOfAttachment = pointOfAttachment;
				init();
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				SpliceLogUtils.trace(LOG, "readExternal");
				super.readExternal(in);
				source = (SpliceOperation) in.readObject();
				emptyRowFunMethodName = readNullableString(in);
				cardinalityCheck = in.readInt();
				subqueryNumber = in.readInt();
				pointOfAttachment = in.readInt();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				SpliceLogUtils.trace(LOG, "writeExternal");
				super.writeExternal(out);
				out.writeObject(source);
				writeNullableString(emptyRowFunMethodName, out);
				out.writeInt(cardinalityCheck);
				out.writeInt(subqueryNumber);
				out.writeInt(pointOfAttachment);
		}

		@Override
		public SpliceOperation getLeftOperation() {
				SpliceLogUtils.trace(LOG,"getLeftOperation");
				return source;
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				super.init(context);
				source.init(context);
				if(emptyRowFun == null) {
						emptyRowFun = new SpliceMethod<ExecRow>(emptyRowFunMethodName, activation);
				}
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
		public List<SpliceOperation> getSubOperations() {
				SpliceLogUtils.trace(LOG, "getSubOperations");
				List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
				operations.add(source);
				return operations;
		}

        @Override
        public int[] getAccessedNonPkColumns() throws StandardException{
            // by default return null
            return source.getAccessedNonPkColumns();
        }


    private static class IteratorRowSource implements RowSource {
        private final Iterator<LocatedRow> iterator;

        public IteratorRowSource(Iterator<LocatedRow> iterator) {
            this.iterator = iterator;
        }

        @Override
        public ExecRow next() throws StandardException, IOException {
            if (iterator.hasNext()) {
                return iterator.next().getRow();
            } else {
                return null;
            }
        }
    }

		@Override
		public String prettyPrint(int indentLevel) {
				String indent = "\n"+ Strings.repeat("\t",indentLevel);

				return new StringBuilder("Once:")
								.append(indent).append("resultSetNumber:").append(resultSetNumber)
								.append(indent).append("emptyRowFunName:").append(emptyRowFunMethodName)
								.append(indent).append("cardinalityCheck:").append(cardinalityCheck)
								.append(indent).append("subqueryNumber:").append(subqueryNumber)
								.append(indent).append("pointOfAttachment:").append(pointOfAttachment)
								.append(indent).append("source:").append(source.prettyPrint(indentLevel+1))
								.toString();
		}

		private ExecRow getRowWithNulls() throws StandardException {
				if (rowWithNulls == null){
						rowWithNulls = emptyRowFun.invoke();
				}
				return rowWithNulls;
		}

		private static interface RowSource{
				ExecRow next() throws StandardException,IOException;
		}
		protected ExecRow validateNextRow(RowSource rowSource,boolean returnNullRow) throws StandardException, IOException {
				ExecRow row = rowSource.next();
                currentRowLocation = source.getCurrentRowLocation();
				if(row!=null){
						switch (cardinalityCheck) {
								case DO_CARDINALITY_CHECK:
								case NO_CARDINALITY_CHECK:
										row = row.getClone();
										if (cardinalityCheck == DO_CARDINALITY_CHECK) {
                    				/* Raise an error if the subquery returns > 1 row
                     				 * We need to make a copy of the current candidateRow since
                     				 * the getNextRow() for this check will wipe out the underlying
                             * row.
                     				 */
												ExecRow secondRow = rowSource.next();
												if(secondRow!=null){
														close();
														throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
												}
										}
										break;
								case UNIQUE_CARDINALITY_CHECK:
										//TODO -sf- I don't think that this will work unless there's a sort order on the first column..
										row = row.getClone();
										DataValueDescriptor orderable1 = row.getColumn(1);

										ExecRow secondRow = rowSource.next();
										while(secondRow!=null){
												DataValueDescriptor orderable2 = secondRow.getColumn(1);
												if (! (orderable1.compare(DataValueDescriptor.ORDER_OP_EQUALS, orderable2, true, true))) {
														close();
														throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
												}
												secondRow = rowSource.next();
										}
										break;
								default:
										if (SanityManager.DEBUG) {
												SanityManager.THROWASSERT(
																"cardinalityCheck not unexpected to be " +
																				cardinalityCheck);
										}
										break;
						}
				}else if(returnNullRow)
						row = getRowWithNulls();

				return row;
		}

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        // We are consuming the dataset, get a resultDataSet
        DataSet<LocatedRow> raw = source.getResultDataSet(dsp);
        final Iterator<LocatedRow> iterator = raw.toLocalIterator();
        ExecRow result;
        try {
            result = validateNextRow(new IteratorRowSource(iterator), true);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }

        return dsp.singleRowDataSet(new LocatedRow(result));
    }

}
