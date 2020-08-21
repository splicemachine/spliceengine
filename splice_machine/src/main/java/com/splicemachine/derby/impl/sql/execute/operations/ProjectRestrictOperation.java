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

import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.HBaseRowLocation;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.db.impl.sql.compile.ExplainNode;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.Restriction;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.ScanInformation;
import com.splicemachine.derby.stream.function.ProjectRestrictMapFunction;
import com.splicemachine.derby.stream.function.ProjectRestrictPredicateFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.EngineUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Strings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;

import static com.splicemachine.db.impl.sql.compile.ExplainNode.SparkExplainKind.NONE;


public class ProjectRestrictOperation extends SpliceBaseOperation {
		private static Logger LOG = Logger.getLogger(ProjectRestrictOperation.class);
		private static int PROJECT_RESTRICT_OPERATION_V2 = 2;
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
		private ExecRow execRowDefinition;
		private ExecRow projRow;
		private String filterPred = null;
		private String[] expressions = null;
		private boolean hasGroupingFunction;
		private String subqueryText;

	    protected static final String NAME = ProjectRestrictOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}

		public boolean hasExpressions() {
		    return (expressions != null && expressions.length > 0);
        }

        public boolean hasFilterPred() {
		    return (filterPred != null && !filterPred.isEmpty());
        }

        public boolean hasGroupingFunction() { return hasGroupingFunction; }

        public String getFilterPred() {
		    if (hasFilterPred())
		        return filterPred;
		    else
		        return "true";
        }

		@SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9844")
		public String[] getExpressions() {
		    return expressions;
        }
		
		@SuppressWarnings("UnusedDeclaration")
		public ProjectRestrictOperation() { super(); }

		@SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "DB-9844")
		public ProjectRestrictOperation(SpliceOperation source,
                                        Activation activation,
                                        GeneratedMethod restriction,
                                        GeneratedMethod projection,
                                        int resultColumnTypeArrayItem,
                                        int resultSetNumber,
                                        GeneratedMethod cr,
                                        int mapRefItem,
                                        int cloneMapItem,
                                        boolean reuseResult,
                                        boolean doesProjection,
                                        double optimizerEstimatedRowCount,
                                        double optimizerEstimatedCost,
                                        String filterPred,
                                        String[] expressions,
				        boolean hasGroupingFunction,
                                        String subqueryText) throws StandardException {
				super(activation,resultColumnTypeArrayItem, resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
				this.restrictionMethodName = (restriction == null) ? null : restriction.getMethodName();
				this.projectionMethodName = (projection == null) ? null : projection.getMethodName();
				this.constantRestrictionMethodName = (cr == null) ? null : cr.getMethodName();
				this.mapRefItem = mapRefItem;
				this.cloneMapItem = cloneMapItem;
				this.reuseResult = reuseResult;
				this.doesProjection = doesProjection;
				this.source = source;
				this.filterPred = filterPred;
				this.expressions = expressions;
				this.hasGroupingFunction = hasGroupingFunction;
				this.subqueryText = subqueryText;
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
				int version = in.readUnsignedByte();
				if (version < PROJECT_RESTRICT_OPERATION_V2)
				    reuseResult = (version == 1);
				else
				    reuseResult = in.readBoolean();
				doesProjection = in.readBoolean();
				source = (SpliceOperation) in.readObject();
				if (version >= PROJECT_RESTRICT_OPERATION_V2) {
				    filterPred = readNullableString(in);
				    int numexpressions = in.readInt();
				    if (numexpressions > 0) {
				        expressions = new String[numexpressions];
				        for (int i = 0; i < numexpressions; i++) {
				            expressions[i] = readNullableString(in);
				        }
				    }
				    hasGroupingFunction = in.readBoolean();
				}
				subqueryText = readNullableString(in);
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				writeNullableString(restrictionMethodName, out);
				writeNullableString(projectionMethodName, out);
				writeNullableString(constantRestrictionMethodName, out);
				out.writeInt(mapRefItem);
				out.writeInt(cloneMapItem);
				out.writeByte(PROJECT_RESTRICT_OPERATION_V2);
				out.writeBoolean(reuseResult);
				out.writeBoolean(doesProjection);
				out.writeObject(source);
				writeNullableString(filterPred, out);
				if (expressions == null)
				    out.writeInt(0);
				else {
				    out.writeInt(expressions.length);
				    for (int i = 0; i < expressions.length; i++) {
				        writeNullableString(expressions[i], out);
				    }
				}
				out.writeBoolean(hasGroupingFunction);
				writeNullableString(subqueryText, out);
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

		@SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", justification = "DB-9844")
		public ExecRow doProjection(ExecRow sourceRow) throws StandardException {
			if (reuseResult && projRow != null)
				return projRow;
            source.setCurrentRow(sourceRow);
			RowLocation rl = new HBaseRowLocation(sourceRow.getKey());
			source.setCurrentRowLocation(rl);
			/* If source is index look-up, we need to set the current row location for the underneath TableScan operation.
			   this is because index look-up is a batch operation, so the rowlocation from teh TableScan may be out-of-sync
			   with the current row being processed.
			 */
			if (source instanceof IndexRowToBaseRowOperation)
				source.getLeftOperation().setCurrentRowLocation(rl);

            ExecRow result;
				if (projection != null) {
						result = projection.invoke();
				} else {
						result = mappedResultRow;
				}
				// Copy any mapped columns from the source
				for (int index = 0; index < projectMapping.length; index++) {
						if (projectMapping[index] != -1) {
								DataValueDescriptor dvd = sourceRow.getColumn(projectMapping[index]);
								// See if the column has been marked for cloning.
								// If the value isn't a stream, don't bother cloning it.
								if (cloneMap[index] && dvd.hasStream()) {
										dvd = dvd.cloneValue(false);
								}
								result.setColumn(index + 1, dvd);
						}
				}
				if (reuseResult)
					projRow = result;
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
						// In case the source's ExecRow has value set to 0 which may cause trouble for division,
					    // set the value to null, the numeric data types have logic to handle null as divisor
                        if(clone!=null) EngineUtils.resultValuesToNull(clone.getRowArray());
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
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
		if (!isOpen)
			throw new IllegalStateException("Operation is not open");

		if (alwaysFalse) {
            return dsp.getEmpty();
        }
        boolean sparkExplainWithSubquery = subqueryText != null && subqueryText.length() != 0;
        OperationContext operationContext = dsp.createOperationContext(this);
	dsp.incrementOpDepth();
	ExplainNode.SparkExplainKind sparkExplainKind = NONE;
	if (sparkExplainWithSubquery) {
	    sparkExplainKind = dsp.getSparkExplainKind();
        }
        DataSet<ExecRow> sourceSet = source.getDataSet(dsp);
        DataSet<ExecRow> originalSourceDataset = sourceSet;
        if (sparkExplainWithSubquery)
            dsp.setSparkExplain(sparkExplainKind);
        dsp.decrementOpDepth();
        try {
            operationContext.pushScope();
            if (restrictionMethodName != null)
                sourceSet = sourceSet.filter(new ProjectRestrictPredicateFunction<>(operationContext));
            DataSet<ExecRow> projection = sourceSet.map(new ProjectRestrictMapFunction<>(operationContext, expressions));

            handleSparkExplain(projection, originalSourceDataset, dsp);
            if (sparkExplainWithSubquery) {
                String lines[] = subqueryText.split("\\r?\\n");
                int increment = 0;
                for (String str:lines) {
                    dsp.appendSpliceExplainString(str);
                    dsp.incrementOpDepth();
                    increment++;
                }
                for (int i = 0; i < increment; i++)
                    dsp.decrementOpDepth();
            }

            return projection;
        } finally {
            operationContext.popScope();
        }
    }

	@Override
	public ExecIndexRow getStartPosition() throws StandardException {
		return source.getStartPosition();
	}

    @Override
    public String getVTIFileName() {
        return getSubOperations().get(0).getVTIFileName();
    }

    @Override
	public FormatableBitSet getAccessedColumns() throws StandardException {
		return source.getAccessedColumns();
	}

	@Override
	public ScanInformation<ExecRow> getScanInformation() {
		return source.getScanInformation();
	}
}
