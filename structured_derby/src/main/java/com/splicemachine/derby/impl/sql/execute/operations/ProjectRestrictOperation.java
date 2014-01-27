package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import com.google.common.base.Strings;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.marshall.*;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class ProjectRestrictOperation extends SpliceBaseOperation {

		private static Logger LOG = Logger.getLogger(ProjectRestrictOperation.class);
		protected String restrictionMethodName;
		protected String projectionMethodName;
		protected String constantRestrictionMethodName;
		protected int mapRefItem;
		protected int cloneMapItem;
		protected boolean reuseResult;
		protected boolean doesProjection;
		protected int[] projectMapping;
		protected ExecRow mappedResultRow;
		protected boolean[] cloneMap;
		protected boolean shortCircuitOpen;
		protected SpliceOperation source;
		protected static List<NodeType> nodeTypes;
		private boolean alwaysFalse;
		protected SpliceMethod<DataValueDescriptor> restriction;
		protected SpliceMethod<ExecRow> projection;
		private ExecRow candidateRow;
		private ExecRow result;
		private boolean restrict;
		private DataValueDescriptor restrictBoolean;

		static {
				nodeTypes = Collections.singletonList(NodeType.MAP);
		}

		public NoPutResultSet[] subqueryTrackingArray;

		public ProjectRestrictOperation() {
				super();
		}

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
				init(SpliceOperationContext.newContext(activation));
				recordConstructorTime();
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
		public void init(SpliceOperationContext context) throws StandardException{
				super.init(context);
				source.init(context);

				GenericStorablePreparedStatement statement = context.getPreparedStatement();
				projectMapping = ((ReferencedColumnsDescriptorImpl) statement.getSavedObject(mapRefItem)).getReferencedColumnPositions();
				if (projectionMethodName == null) {
						mappedResultRow = activation.getExecutionFactory().getValueRow(projectMapping.length);
				}
				cloneMap = ((boolean[])statement.getSavedObject(cloneMapItem));
				if (this.constantRestrictionMethodName != null) {
						SpliceMethod<DataValueDescriptor> constantRestriction = new SpliceMethod<DataValueDescriptor>(constantRestrictionMethodName,activation);
						DataValueDescriptor restrictBoolean = constantRestriction.invoke();
						shortCircuitOpen  = (restrictBoolean == null) || ((!restrictBoolean.isNull()) && restrictBoolean.getBoolean());

						alwaysFalse = restrictBoolean != null && !restrictBoolean.isNull() && !restrictBoolean.getBoolean();

				}
				if (restrictionMethodName != null)
						restriction = new SpliceMethod<DataValueDescriptor>(restrictionMethodName,activation);
				if (projectionMethodName != null)
						projection = new SpliceMethod<ExecRow>(projectionMethodName,activation);
				startExecutionTime = System.currentTimeMillis();
		}


		@Override
		public List<SpliceOperation> getSubOperations() {
				return Arrays.asList(source);
		}


		@Override
		public SpliceOperation getLeftOperation() {
				return source;
		}

		@Override
		public List<NodeType> getNodeTypes() {
				return nodeTypes;
		}

		private ExecRow doProjection(ExecRow sourceRow) throws StandardException {
				ExecRow result;
				if (projection != null) {
						result = projection.invoke();
				} else {
						result = mappedResultRow;
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

	    /* We need to reSet the current row after doing the projection */
				setCurrentRow(result);
        /* Remember the result if reusing it */
				return result;
		}

		@Override
		public NoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				RowProvider provider = getReduceRowProvider(this,OperationUtils.getPairDecoder(this,runtimeContext),runtimeContext);
				SpliceNoPutResultSet rs =  new SpliceNoPutResultSet(activation,this, provider);
				nextTime += getCurrentTimeMillis() - beginTime;
				return rs;
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				return source.getMapRowProvider(top, decoder, spliceRuntimeContext);
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				return source.getReduceRowProvider(top, decoder, spliceRuntimeContext);
		}


		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(alwaysFalse){
						return null;
				}
				if(timer==null)
						timer = spliceRuntimeContext.newTimer();

				candidateRow = null;
				result = null;
				restrict = false;
				restrictBoolean = null;

				timer.startTiming();
				do {
						candidateRow = source.nextRow(spliceRuntimeContext);
						if (LOG.isTraceEnabled())
								SpliceLogUtils.trace(LOG, ">>>   ProjectRestrictOp[%d]: Candidate: %s", Bytes.toLong(uniqueSequenceID), candidateRow);
						if (candidateRow != null) {
								inputRows++;
								/* If restriction is null, then all rows qualify */
								if (restriction == null) {
										restrict = true;
								} else {
										setCurrentRow(candidateRow);
										restrictBoolean = restriction.invoke();
										// if the result is null, we make it false --
										// so the row won't be returned.
										restrict = ((! restrictBoolean.isNull()) && restrictBoolean.getBoolean());
										if (! restrict) {
												if (LOG.isTraceEnabled())
														SpliceLogUtils.trace(LOG, ">>>   ProjectRestrictOp[%d]: Candidate Filtered: %s",Bytes.toLong(uniqueSequenceID),candidateRow);
												rowsFiltered++;
										}
								}
						}
				} while ( (candidateRow != null) && (! restrict ) );
				if (candidateRow != null)  {
						result = doProjection(candidateRow);
						if (LOG.isTraceEnabled())
								SpliceLogUtils.trace(LOG, ">>>   ProjectRestrictOp[%d] Result: %s",Bytes.toLong(uniqueSequenceID), result);
				} else {
					/* Clear the current row, if null */
						clearCurrentRow();
				}
				currentRow = result;
				setCurrentRow(currentRow);
				if (statisticsTimingOn) {
						if (! isTopResultSet) {
								/* This is simply for RunTimeStats */
								//TODO: need to getStatementContext() from somewhere
								if (activation.getLanguageConnectionContext().getStatementContext() == null)
										SpliceLogUtils.trace(LOG, "Cannot get StatementContext from Activation's lcc");
								else
										subqueryTrackingArray = activation.getLanguageConnectionContext().getStatementContext().getSubqueryTrackingArray();
						}
						nextTime += getElapsedMillis(beginTime);
				}
				if(result==null){
						timer.tick(1);
						stopExecutionTime = System.currentTimeMillis();
				}else timer.tick(0);
				return result;
		}

		@Override
		public ExecRow getExecRowDefinition() throws StandardException {
				ExecRow def = source.getExecRowDefinition();
				if (def != null) SpliceUtils.populateDefaultValues(def.getRowArray(),1);
				source.setCurrentRow(def);
				return doProjection(def);
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

		@Override protected int getNumMetrics() { return 1; }

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				stats.addMetric(OperationMetric.FILTERED_ROWS,rowsFiltered);
				stats.addMetric(OperationMetric.INPUT_ROWS,inputRows);
		}

		@Override
		public void open() throws StandardException, IOException {
				super.open();
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG,">>>   ProjectRestrictOp: Opening ",(source != null ? source.getClass().getSimpleName() : "null source"));
				if(source!=null)source.open();
		}

		@Override
		public void	close() throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "close in ProjectRestrict");
				/* Nothing to do if open was short circuited by false constant expression */
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, ">>>   ProjectRestrictOp: Closing ", (source != null ? source.getClass().getSimpleName() : "null source"));
				super.close();
				source.close();
				closeTime += getElapsedMillis(beginTime);
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
}
