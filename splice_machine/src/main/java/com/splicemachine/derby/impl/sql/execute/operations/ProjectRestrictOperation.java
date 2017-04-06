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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import org.spark_project.guava.base.Strings;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.Restriction;
import com.splicemachine.derby.stream.function.ProjectRestrictFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.derby.utils.EngineUtils;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;

public class ProjectRestrictOperation extends SpliceBaseOperation {
		private static Logger LOG = Logger.getLogger(ProjectRestrictOperation.class);
		protected String restrictionMethodName;
		protected String projectionMethodName;
		protected String constantRestrictionMethodName;
		protected int mapRefItem;
		protected int cloneMapItem;
		protected boolean reuseResult;
		protected boolean doesProjection;
		public int[] projectMapping;
		public ExecRow mappedResultRow;
		public boolean[] cloneMap;
		protected boolean shortCircuitOpen;
		public SpliceOperation source;
		private boolean alwaysFalse;
		public SpliceMethod<DataValueDescriptor> restriction;
		public SpliceMethod<ExecRow> projection;
        public ExecRow projectionResult;
		public NoPutResultSet[] subqueryTrackingArray;
		private ExecRow execRowDefinition;

	    protected static final String NAME = ProjectRestrictOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}

		
		@SuppressWarnings("UnusedDeclaration")
		public ProjectRestrictOperation() { super(); }

		public ProjectRestrictOperation(SpliceOperation source,
																		Activation activation,
																		GeneratedMethod restriction,
																		GeneratedMethod projection,
																		int resultSetNumber,
																		GeneratedMethod cr,
																		int mapRefItem,
																		int cloneMapItem,
																		boolean reuseResult,
																		boolean doesProjection,
																		double optimizerEstimatedRowCount,
																		double optimizerEstimatedCost) throws StandardException {
				super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
				this.restrictionMethodName = (restriction == null) ? null : restriction.getMethodName();
				this.projectionMethodName = (projection == null) ? null : projection.getMethodName();
				this.constantRestrictionMethodName = (cr == null) ? null : cr.getMethodName();
				this.mapRefItem = mapRefItem;
				this.cloneMapItem = cloneMapItem;
				this.reuseResult = reuseResult;
				this.doesProjection = doesProjection;
				this.source = source;
				init();
		}

		public String getRestrictionMethodName() {
				return restrictionMethodName;
		}

		public boolean doesProjection() {
				return doesProjection;
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
				super.readExternal(in);
				restrictionMethodName = readNullableString(in);
				projectionMethodName = readNullableString(in);
				constantRestrictionMethodName = readNullableString(in);
				mapRefItem = in.readInt();
				cloneMapItem = in.readInt();
				reuseResult = in.readBoolean();
				doesProjection = in.readBoolean();
				source = (SpliceOperation) in.readObject();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				writeNullableString(restrictionMethodName, out);
				writeNullableString(projectionMethodName, out);
				writeNullableString(constantRestrictionMethodName, out);
				out.writeInt(mapRefItem);
				out.writeInt(cloneMapItem);
				out.writeBoolean(reuseResult);
				out.writeBoolean(doesProjection);
				out.writeObject(source);
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				super.init(context);
				source.init(context);

				GenericStorablePreparedStatement statement = context.getPreparedStatement();
				projectMapping = ((ReferencedColumnsDescriptorImpl) statement.getSavedObject(mapRefItem)).getReferencedColumnPositions();
				if (projectionMethodName == null) {
						mappedResultRow = activation.getExecutionFactory().getValueRow(projectMapping.length);
				}
				cloneMap = ((boolean[])statement.getSavedObject(cloneMapItem));
				if (this.constantRestrictionMethodName != null) {
						SpliceMethod<DataValueDescriptor> constantRestriction =new SpliceMethod<>(constantRestrictionMethodName,activation);
						DataValueDescriptor restrictBoolean = constantRestriction.invoke();
						shortCircuitOpen  = (restrictBoolean == null) || ((!restrictBoolean.isNull()) && restrictBoolean.getBoolean());

						alwaysFalse = restrictBoolean != null && !restrictBoolean.isNull() && !restrictBoolean.getBoolean();

				}
				if (restrictionMethodName != null)
						restriction =new SpliceMethod<>(restrictionMethodName,activation);
				if (projectionMethodName != null)
						projection =new SpliceMethod<>(projectionMethodName,activation);
		}


		@Override
		public List<SpliceOperation> getSubOperations() {
				return Arrays.asList(source);
		}


		@Override
		public SpliceOperation getLeftOperation() {
				return source;
		}

		public ExecRow doProjection(ExecRow sourceRow) throws StandardException {
            source.setCurrentRow(sourceRow);
            ExecRow result;
				if (projection != null) {
						result = projection.invoke();
				} else {
						result = mappedResultRow.getNewNullRow();
				}
				// Copy any mapped columns from the source
				for (int index = 0; index < projectMapping.length; index++) {
						if (sourceRow != null && projectMapping[index] != -1) {
								DataValueDescriptor dvd = sourceRow.getColumn(projectMapping[index]);
								// See if the column has been marked for cloning.
								// If the value isn't a stream, don't bother cloning it.
								if (cloneMap[index] && dvd.hasStream()) {
										dvd = dvd.cloneValue(false);
								}
								result.setColumn(index + 1, dvd);
						}
				}
        /* Remember the result if reusing it */
				return result;
		}

		/**
		 * Returns the definition of the columns in a row.  A projection will be executed to determine the columns
		 * that are added or removed.  Default values are used for the column values.
		 * PLEASE NOTE: Numeric columns will be ones by default.  So the delegate operation may need to reset the values
		 * to zeroes, etc.  This is what happens in the ScalarAggregateOperation classes.
		 *
		 * @return the definition of the row
		 *
		 * @throws StandardException
		 */
		@Override
		public ExecRow getExecRowDefinition() throws StandardException {

				if(execRowDefinition==null){
						ExecRow def = source.getExecRowDefinition();
						ExecRow clone = def !=null? def.getClone(): null;
						// Set the default values to 1.  This is to avoid division by zero if any of the projected columns have
						// division or modulus operators.  The delegate classes will need to reset the values to 0.
                        if(clone!=null) EngineUtils.populateDefaultValues(clone.getRowArray(),1);
                        // Keeps Sequences from incrementing when doing an execRowDefiniton evaluation: hack
                        ((BaseActivation) activation).setIgnoreSequence(true);
						source.setCurrentRow(clone);
						execRowDefinition = doProjection(clone);
                    ((BaseActivation) activation).setIgnoreSequence(false);
                }
				execRowDefinition = execRowDefinition.getClone();
                EngineUtils.resultValuesToNull(execRowDefinition.getRowArray());
				return execRowDefinition;
		}

		@Override
		public String toString() {
				return String.format("ProjectRestrictOperation {source=%s,resultSetNumber=%d}",source,resultSetNumber);
		}

		@Override
		public int[] getRootAccessedCols(long tableNumber) throws StandardException {
				int[] sourceCols = source.getRootAccessedCols(tableNumber);
				if (projectMapping == null) {
						return sourceCols;
				}
				int[] result = new int[projectMapping.length];
				for (int i = 0; i < projectMapping.length; ++i) {
						if (projectMapping[i] > 0) {
								result[i] = sourceCols[projectMapping[i] - 1];
						}
				}
				return result;
		}

		@Override
		public boolean isReferencingTable(long tableNumber){
				return source.isReferencingTable(tableNumber);
		}

		public SpliceOperation getSource() {
				return this.source;
		}

		@Override
		public String prettyPrint(int indentLevel) {
				String indent = "\n"+ Strings.repeat("\t",indentLevel);

				return "ProjectRestrict:" + indent
								+ "resultSetNumber:" + resultSetNumber + indent
								+ "restrictionMethodName:" + restrictionMethodName + indent
								+ "projectionMethodName:" + projectionMethodName + indent
								+ "doesProjection:" + doesProjection + indent
								+ "source:" + source.prettyPrint(indentLevel + 1);
		}

    public Restriction getRestriction() {
        Restriction mergeRestriction = Restriction.noOpRestriction;
        if (restriction != null) {
            mergeRestriction = new Restriction() {
                @Override
                public boolean apply(ExecRow row) throws StandardException {
                    activation.setCurrentRow(row, resultSetNumber);
                    DataValueDescriptor shouldKeep = restriction.invoke();
                    return !shouldKeep.isNull() && shouldKeep.getBoolean();
                }
            };
        }
        return mergeRestriction;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (alwaysFalse) {
            return dsp.getEmpty();
        }
        OperationContext operationContext = dsp.createOperationContext(this);
        DataSet<LocatedRow> sourceSet = source.getDataSet(dsp);
        try {
            operationContext.pushScope();
            return sourceSet.flatMap(new ProjectRestrictFlatMapFunction<SpliceOperation>(operationContext), true);
        } finally {
            operationContext.popScope();
        }
    }


    public static ExecRow copyProjectionToNewRow(ExecRow projectedRow, ExecRow newRow) {
        if (newRow == null) {
            return null;
        }
        DataValueDescriptor[] projectRowArray = projectedRow.getRowArray();
        DataValueDescriptor[] rightRowArray = newRow.getRowArray();
        System.arraycopy(projectRowArray, 0, rightRowArray, 0, projectRowArray.length);
        return newRow;
    }
	@Override
	public ExecIndexRow getStartPosition() throws StandardException {
		return source.getStartPosition();
	}

    @Override
    public String getVTIFileName() {
        return getSubOperations().get(0).getVTIFileName();
    }
}
