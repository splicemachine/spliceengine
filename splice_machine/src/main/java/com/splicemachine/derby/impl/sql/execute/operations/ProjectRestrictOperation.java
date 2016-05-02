package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Strings;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.impl.spark.RDDUtils;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.marshall.*;

import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.spark.api.java.JavaRDD;

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
        private ExecRow projectionResult;

		static {
				nodeTypes = Collections.singletonList(NodeType.MAP);
		}

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
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
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
		public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				try {
						RowProvider provider = getReduceRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext),runtimeContext, true);
						SpliceNoPutResultSet rs =  new SpliceNoPutResultSet(activation,this, provider);
						nextTime += getCurrentTimeMillis() - beginTime;
						return rs;
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				return source.getMapRowProvider(top, decoder, spliceRuntimeContext);
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				return source.getReduceRowProvider(top, decoder, spliceRuntimeContext, returnDefaultValue);
		}


		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(alwaysFalse){
						return null;
				}
				if(timer==null){
						timer = spliceRuntimeContext.newTimer();
						timer.startTiming();
				}

				ExecRow candidateRow;
				ExecRow result = null;
				boolean restrict = false;
				DataValueDescriptor restrictBoolean;

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
                                        if (activation.isTraced()) {
                                            // Push the operation Id
                                            addToOperationChain(spliceRuntimeContext, "Subquery:" + restrictionMethodName, uniqueSequenceID);
                                        }
										setCurrentRow(candidateRow);
										try {
											restrictBoolean = restriction.invoke();
										} catch (Exception e) {
											System.out.println(candidateRow);
											throw new IOException(e);
										}
										// if the result is null, we make it false --
										// so the row won't be returned.
										restrict = ((! restrictBoolean.isNull()) && restrictBoolean.getBoolean());
										if (!restrict) {
												if (LOG.isTraceEnabled())
														SpliceLogUtils.trace(LOG, ">>>   ProjectRestrictOp[%d]: Candidate Filtered: %s",Bytes.toLong(uniqueSequenceID), candidateRow);
												rowsFiltered++;
										}
                                        if(candidateRow != null && !restrict && activation.isTraced()) {
                                            removeFromOperationChain();
                                        }
								}
						}
				} while ( (candidateRow != null) && (!restrict) );
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
						/*if (! isTopResultSet) {
								// This is simply for RunTimeStats
								//TODO: need to getStatementContext() from somewhere
								if (activation.getLanguageConnectionContext().getStatementContext() == null)
										SpliceLogUtils.trace(LOG, "Cannot get StatementContext from Activation's lcc");
								else
										subqueryTrackingArray = activation.getLanguageConnectionContext().getStatementContext().getSubqueryTrackingArray();
						}*/
                    // Remove the last emelemt in the chain since this operation is exiting
                    removeFromOperationChain();
                    nextTime += getElapsedMillis(beginTime);
				}
				if(result ==null){
						timer.stopTiming();
						stopExecutionTime = System.currentTimeMillis();
				}
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
						if(clone!=null) SpliceUtils.populateDefaultValues(clone.getRowArray(),1);
						source.setCurrentRow(clone);
						execRowDefinition = doProjection(clone);
				}
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

		@Override protected int getNumMetrics() { return 1; }

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				stats.addMetric(OperationMetric.FILTERED_ROWS,rowsFiltered);
				stats.addMetric(OperationMetric.INPUT_ROWS,inputRows);
            stats.addMetric(OperationMetric.OUTPUT_ROWS,inputRows-rowsFiltered);
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
        operationChain.remove(); //we are done here
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

    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {
        if (alwaysFalse) {
            return SpliceSpark.getContext().parallelize(Collections.<LocatedRow>emptyList());
        }
        JavaRDD<LocatedRow> raw = source.getRDD(spliceRuntimeContext, top);
        if (pushedToServer()) {
            // we want to avoid re-applying the PR if it has already been executed in HBase
            return raw;
        }
        final SpliceObserverInstructions soi = SpliceObserverInstructions.create(activation, this, spliceRuntimeContext);
        JavaRDD<LocatedRow> projected = raw.mapPartitions(new ProjectRestrictSparkOp(this, soi));
        return projected;
    }

    @Override
    public boolean providesRDD() {
        return source.providesRDD();
    }

    @Override
    public boolean pushedToServer() {
        return source.pushedToServer();
    }

    public static final class ProjectRestrictSparkOp extends SparkFlatMapOperation<ProjectRestrictOperation, Iterator<LocatedRow>, LocatedRow> {
        public ProjectRestrictSparkOp() {
        }

        public ProjectRestrictSparkOp(ProjectRestrictOperation spliceOperation, SpliceObserverInstructions soi) {
            super(spliceOperation, soi);
        }

        public LocatedRow project(LocatedRow sourceRow) throws Exception {
            ExecRow result;

            op.source.setCurrentRow(sourceRow.getRow());
            op.source.setCurrentRowLocation(sourceRow.getRowLocation());

            if (op.restriction != null) {
                DataValueDescriptor restrictBoolean = (DataValueDescriptor) op.restriction.invoke();
                // if the result is null, we make it false --
                // so the row won't be returned.
                boolean restrict = ((! restrictBoolean.isNull()) && restrictBoolean.getBoolean());
                if (RDDUtils.LOG.isDebugEnabled()) {
                    RDDUtils.LOG.debug("restricted row " + sourceRow + ": " + restrict);
                }
                if (!restrict)
                    return null; // filter out this row
            }

            if (op.projection != null) {
                ExecRow tmp = (ExecRow) op.projection.invoke();
                if (op.projectionResult == null || tmp == op.projectionResult) {
                    result = tmp.getClone();
                } else {
                    result = tmp;
                }
                op.projectionResult = tmp;
            } else {
                result = op.mappedResultRow.getNewNullRow();
            }
            // Copy any mapped columns from the source
            for (int index = 0; index < op.projectMapping.length; index++) {
                if (sourceRow != null && op.projectMapping[index] != -1) {
                    DataValueDescriptor dvd = sourceRow.getRow().getColumn(op.projectMapping[index]);
                    // See if the column has been marked for cloning.
                    // If the value isn't a stream, don't bother cloning it.
                    if (op.cloneMap[index] && dvd.hasStream()) {
                        dvd = dvd.cloneValue(false);
                    }
                    result.setColumn(index + 1, dvd);
                }
            }
            activation.setCurrentRow(result, op.resultSetNumber);
            if (RDDUtils.LOG.isDebugEnabled()) {
                RDDUtils.LOG.debug("Projected " + sourceRow + " into " + result);
            }
            return new LocatedRow(sourceRow.getRowLocation(), result);
        }

        @Override
        public Iterable<LocatedRow> call(Iterator<LocatedRow> source) throws Exception {
            return new IteratorWithContext(source);
        }

        private class IteratorWithContext implements Iterable<LocatedRow>, Iterator<LocatedRow> {
            private final Iterator<LocatedRow> source;
            private boolean populated;
            private LocatedRow next;
            private boolean prepared = false;
            private boolean closed = false;

            public IteratorWithContext(Iterator<LocatedRow> source) {
                this.source = source;
            }

            @Override
            public Iterator<LocatedRow> iterator() {
                return this;
            }

            @Override
            public boolean hasNext() {
                if (closed)
                    return false;
                if (populated)
                    return true;
                try {
                    if (!prepared) {
                        impl.prepareContextManager();
                        prepared = true;
                    }
                    next = null;
                    while(next == null && source.hasNext()) {
                        LocatedRow r = source.next();
                        next = project(r);
                    }
                } catch (Exception e) {
                    if (prepared) {
                        closed = true;
                        impl.resetContextManager();
                    }
                    throw new RuntimeException(e);
                }
                populated = next != null;
                if (!populated) {
                    closed = true;
                    impl.resetContextManager();
                }
                return populated;
            }

            @Override
            public LocatedRow next() {
                if (hasNext())  {
                    populated = false;
                    LocatedRow result = next;
                    next = null;
                    return result;
                }
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }
    }
	@Override
	public ExecIndexRow getStartPosition() throws StandardException {
		return source.getStartPosition();
	}

	@Override
	public String getOptimizerOverrides(){
		return source.getOptimizerOverrides();
	}
}
