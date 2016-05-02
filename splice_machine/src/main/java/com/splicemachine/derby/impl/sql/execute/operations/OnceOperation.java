package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.spark.RDDRowProvider;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobResults;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.utils.SpliceLogUtils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.shared.common.sanity.SanityManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

public class OnceOperation extends SpliceBaseOperation {
		private static final long serialversionUID = 1l;
		private static Logger LOG = Logger.getLogger(OnceOperation.class);
		public static final int DO_CARDINALITY_CHECK		= 1;
		public static final int NO_CARDINALITY_CHECK		= 2;
		public static final int UNIQUE_CARDINALITY_CHECK	= 3;
		private ExecRow rowWithNulls;

		/* Used to cache the StatementContext */
		private StatementContext statementContext;

		// set in constructor and not altered during
		// life of object.
		public SpliceOperation source;
		protected SpliceMethod<ExecRow> emptyRowFun;
		protected String emptyRowFunMethodName;
		private int cardinalityCheck;
		public int subqueryNumber;
		public int pointOfAttachment;

		private RowSource rowSource;

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
				try {
						init(SpliceOperationContext.newContext(a));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				recordConstructorTime();
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
		public ExecRow nextRow(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {

                if (timer == null) {
                    timer = spliceRuntimeContext.newTimer();
                }
                timer.startTiming();
                if(rowSource==null){
						rowSource = new RowSource(){

								@Override
								public ExecRow next() throws StandardException, IOException {
										return source.nextRow(spliceRuntimeContext);
								}
						};
				}

				ExecRow next = validateNextRow(rowSource,false);

				//do null-filling on the other side of the serialization barrier
				setCurrentRow(next);
                if (next == null) {
                    timer.tick(0);
                }
                else {
                    timer.tick(1);
                }
                timer.stopTiming();
				return next;
		}

        @Override
        protected void updateStats(OperationRuntimeStats stats) {
            stats.addMetric(OperationMetric.INPUT_ROWS, timer.getNumEvents());
            stats.addMetric(OperationMetric.OUTPUT_ROWS, timer.getNumEvents());
        }

		@Override
		public void close() throws StandardException, IOException {
				source.close();
				super.close();
		}

		@Override
		public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG, "executeScan");
				final List<SpliceOperation> operationStack =getOperationStack();
				SpliceLogUtils.trace(LOG, "operationStack=%s",operationStack);
				SpliceOperation regionOperation = operationStack.get(0);
				SpliceLogUtils.trace(LOG,"regionOperation=%s",regionOperation);
				try {
						RowProvider provider = getReduceRowProvider(this, OperationUtils.getPairDecoder(source, runtimeContext),runtimeContext, true);
						return new SpliceNoPutResultSet(activation,this, provider);
				} catch (IOException e) {
						throw Exceptions.parseException(e);
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
		public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "getMapRowProvider");
				return source.getMapRowProvider(top,rowDecoder,spliceRuntimeContext);
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "getReduceRowProvider");
				return new OnceRowProvider(source.getReduceRowProvider(top,rowDecoder, spliceRuntimeContext, returnDefaultValue));
		}

		@Override
		public List<NodeType> getNodeTypes() {
				SpliceLogUtils.trace(LOG, "getNodeTypes");
				return Collections.singletonList(NodeType.SCAN);
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

//	@Override
//	public long getTimeSpent(int type)
//	{
//		long totTime = constructorTime + openTime + nextTime + closeTime;
//
//		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
//			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
//		else
//			return totTime;
//	}


		@Override
		public void open() throws StandardException, IOException {
				super.open();
				if(source!=null)source.open();
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

    private class OnceRowProvider implements RowProvider {
				private final RowProvider delegate;
				private ExecRow nextRow;
				private final RowSource rowSource;

				public OnceRowProvider(final RowProvider delegate) {
						this.delegate = delegate;
						this.rowSource = new RowSource() {
								@Override
								public ExecRow next() throws StandardException, IOException {
										return delegate.hasNext()? delegate.next(): null;
								}
						};
				}

				@Override public void open() throws StandardException {
						delegate.open();
				}
				@Override public void close() throws StandardException { delegate.close(); }
				@Override public RowLocation getCurrentRowLocation() { return delegate.getCurrentRowLocation(); }
				@Override public byte[] getTableName() { return delegate.getTableName(); }
				@Override public int getModifiedRowCount() { return delegate.getModifiedRowCount(); }

				@Override
				public JobResults shuffleRows(SpliceObserverInstructions instructions, Callable<Void>... postCompleteTasks) throws StandardException, IOException {
						return delegate.shuffleRows(instructions,postCompleteTasks);
				}

				@Override
				public List<Pair<JobFuture,JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException, IOException {
						return delegate.asyncShuffleRows(instructions);
				}

				@Override
				public JobResults finishShuffle(List<Pair<JobFuture, JobInfo>> jobFutures, Callable<Void>... intermediateCleanupTasks) throws StandardException {
						return delegate.finishShuffle(jobFutures,intermediateCleanupTasks);
				}

				@Override
				public boolean hasNext() throws StandardException, IOException {
						/*
						 * We have to do our cardinality checks here
						 *
						 * The reason for this is simple. Suppose you have a Union all as the source for this. That
						 * union will return rows from multiple tables, which will require multiple serialization points (1
						 * for each table). If we serialize this operation over to the table responsible, then we will
						 * serialize twice, and each table will only see 1 row, in which case we will pass a query when we shouldn't.
						 *
						 * Also, conceptually, that makes sense--this is a verification node--it verifies output, it doesn't
						 * produce it's own (not really, anyway). Thus, it really shouldn't be part of the output itself.
						 */
						nextRow = validateNextRow(rowSource,true);

						// OnceOp always has another row…
						return true;
				}

				@Override
				public ExecRow next() throws StandardException {
						return nextRow;
				}

				@Override
				public SpliceRuntimeContext getSpliceRuntimeContext() {
						return delegate.getSpliceRuntimeContext();
				}


				@Override
				public void reportStats(long statementId, long operationId, long taskId, String xplainSchema,String regionName) throws IOException {
					delegate.reportStats(statementId,operationId,taskId,xplainSchema,regionName);
				}

				@Override public IOStats getIOStats() { return delegate.getIOStats(); }
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

    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {
        JavaRDD<LocatedRow> raw = source.getRDD(spliceRuntimeContext, top);
        if (pushedToServer()) {
            // we want to avoid re-applying the OnceOp if it has already been executed in HBase
            return raw;
        }
        final Iterator<LocatedRow> iterator = raw.toLocalIterator();
        ExecRow result;
        try {
            result = validateNextRow(new IteratorRowSource(iterator), false);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        return SpliceSpark.getContext().parallelize(Lists.newArrayList(new LocatedRow(result)));
    }

    @Override
    public boolean providesRDD() {
        return source.providesRDD();
    }


    @Override
    public boolean pushedToServer() {
        return source.pushedToServer();
    }

    @Override
    public String toString() {
        return String.format("OnceOperation {source=%s,resultSetNumber=%d}",source,resultSetNumber);
    }

    @Override
    public SpliceNoPutResultSet executeRDD(SpliceRuntimeContext runtimeContext) throws StandardException {
        JavaRDD<LocatedRow> rdd = getRDD(runtimeContext, this);
        if (LOG.isInfoEnabled()) {
            LOG.info("RDD for operation " + this + " :\n " + rdd.toDebugString());
        }
        return new SpliceNoPutResultSet(getActivation(), this,
                new RDDRowProvider(rdd, runtimeContext){

                    @Override
                    public boolean hasNext() throws StandardException, IOException {
                        /*
						 * We have to do our cardinality checks here
						 *
						 * The reason for this is simple. Suppose you have a Union all as the source for this. That
						 * union will return rows from multiple tables, which will require multiple serialization points (1
						 * for each table). If we serialize this operation over to the table responsible, then we will
						 * serialize twice, and each table will only see 1 row, in which case we will pass a query when we shouldn't.
						 *
						 * Also, conceptually, that makes sense--this is a verification node--it verifies output, it doesn't
						 * produce it's own (not really, anyway). Thus, it really shouldn't be part of the output itself.
						 */
                        currentRow = validateNextRow(new IteratorRowSource(iterator), true);

                        // OnceOp always has another row…
                        return true;
                    }
                });
    }

	@Override
	public String getOptimizerOverrides(SpliceRuntimeContext ctx){
		return source.getOptimizerOverrides(ctx);
	}
}
