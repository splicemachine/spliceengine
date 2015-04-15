package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.OperationResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.metrics.*;
import com.splicemachine.utils.SpliceLogUtils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NestedLoopJoinOperation extends JoinOperation {
		private static Logger LOG = Logger.getLogger(NestedLoopJoinOperation.class);
    protected NestedLoopIterator nestedLoopIterator;
		protected boolean isHash;
		protected static List<NodeType> nodeTypes;
        protected byte[] rightResultSetUniqueSequenceID;

        protected static final String NAME = NestedLoopJoinOperation.class.getSimpleName().replaceAll("Operation","");

    	@Override
    	public String getName() {
    			return NAME;
    	}

        
		static {
				nodeTypes = new ArrayList<NodeType>();
				nodeTypes.add(NodeType.MAP);
				nodeTypes.add(NodeType.SCROLL);
		}

		private long taskId;
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
																	 double optimizerEstimatedRowCount,
																	 double optimizerEstimatedCost,
																	 String userSuppliedOptimizerOverrides) throws StandardException {
				super(leftResultSet,leftNumCols,rightResultSet,rightNumCols,activation,restriction,
								resultSetNumber,oneRowRightSide,notExistsRightSide,optimizerEstimatedRowCount,
								optimizerEstimatedCost,userSuppliedOptimizerOverrides);
				this.isHash = false;
        try {
            init(SpliceOperationContext.newContext(activation,null));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        recordConstructorTime();
		}

		@Override
		public List<NodeType> getNodeTypes() {
				return nodeTypes;
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
				startExecutionTime = System.currentTimeMillis();
		}

		@Override
		public String toString() {
				return "NestedLoop"+super.toString();
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				return next(false, spliceRuntimeContext);
		}

		protected ExecRow leftNext(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				return leftResultSet.nextRow(spliceRuntimeContext);
		}

		protected ExecRow next(boolean outerJoin, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(timer==null){
						timer = spliceRuntimeContext.newTimer();
						if(spliceRuntimeContext.shouldRecordTraceMetrics()){
								byte[] task = spliceRuntimeContext.getCurrentTaskId();
								if(task!=null){
										taskId = Bytes.toLong(task);
								}else{
										taskId = SpliceDriver.driver().getUUIDGenerator().nextUUID();
								}

                                activation.getLanguageConnectionContext().setStatisticsTiming(true);
                              }
				}
                if (rightResultSetUniqueSequenceID == null) {
                    rightResultSetUniqueSequenceID = rightResultSet.getUniqueSequenceID();
                }
				timer.startTiming();
				// loop until NL iterator produces result, or until left side exhausted
				while ((nestedLoopIterator == null || !nestedLoopIterator.hasNext()) && ( (leftRow = leftNext(spliceRuntimeContext)) != null) ){
						if (nestedLoopIterator != null){
								nestedLoopIterator.close();
						}
						leftResultSet.setCurrentRow(leftRow);
						rowsSeenLeft++;
						SpliceLogUtils.debug(LOG, ">>>  NestdLoopJoin: new NLIterator");
						nestedLoopIterator = new NestedLoopIterator(leftRow, isHash, outerJoin, rightResultSetUniqueSequenceID, spliceRuntimeContext, true, false);
				}
				if (leftRow == null){
						if(nestedLoopIterator!=null){
								nestedLoopIterator.close();
								nestedLoopIterator=null;
						}

						mergedRow = null;
						timer.stopTiming();
						stopExecutionTime = System.currentTimeMillis();
						setCurrentRow(mergedRow);
						return mergedRow;
				} else {
						ExecRow next = nestedLoopIterator.next();
						SpliceLogUtils.debug(LOG, ">>>  NestdLoopJoin: Next: ",(next != null ? next : "NULL Next Row"));
						setCurrentRow(next);
						timer.tick(1);
						return next;
				}
		}

		@Override
		public void	close() throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "close in NestdLoopJoin");
				beginTime = getCurrentTimeMillis();
				if(nestedLoopIterator!=null){
						nestedLoopIterator.close();
						nestedLoopIterator=null;
				}
				clearCurrentRow();
				super.close();
				closeTime += getElapsedMillis(beginTime);
		}

		@Override
		public String prettyPrint(int indentLevel) {
				return "NestedLoopJoin:" + super.prettyPrint(indentLevel);
		}


		@Override
		protected void updateStats(OperationRuntimeStats stats) {
            stats.addMetric(OperationMetric.INPUT_ROWS,rowsSeenLeft);
            if (timer != null) {
                stats.addMetric(OperationMetric.OUTPUT_ROWS, timer.getNumEvents());
            }
            stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,rowsSeenRight);
            stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES, bytesReadRight);
            stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME, remoteScanWallTime);
            stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME, remoteScanCpuTime);
            stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME, remoteScanUserTime);
            super.updateStats(stats);
		}

    protected NestedLoopIterator createNestedLoopIterator(ExecRow leftRow, boolean hash, SpliceRuntimeContext spliceRuntimeContext, boolean showStatementInfo, boolean cloneResults) throws StandardException, IOException {
        if (rightResultSetUniqueSequenceID == null) {
            rightResultSetUniqueSequenceID = rightResultSet.getUniqueSequenceID();
        }
        return new NestedLoopIterator(leftRow, hash, false, rightResultSetUniqueSequenceID, spliceRuntimeContext, showStatementInfo, cloneResults);
    }

    protected class NestedLoopIterator implements Iterator<ExecRow>, Iterable<ExecRow> {
        protected ExecRow leftRow;
        protected OperationResultSet probeResultSet;
        private boolean populated;
        private boolean returnedRight=false;
        private boolean outerJoin = false;
        private boolean clone = false;


        NestedLoopIterator(ExecRow leftRow, boolean hash, boolean outerJoin, byte[] rightResultSetUniqueSequenceID,
                           SpliceRuntimeContext spliceRuntimeContext, boolean showStatementInfo, boolean cloneResults) throws StandardException, IOException {
            SpliceLogUtils.trace(LOG, "NestedLoopIterator instantiated with leftRow %s",leftRow);
            this.leftRow = leftRow;
            probeResultSet = getRightResultSet();
            if (shouldRecordStats()) {
                addToOperationChain(spliceRuntimeContext, null, rightResultSetUniqueSequenceID);
            }
            SpliceRuntimeContext ctx = probeResultSet.sinkOpen(spliceRuntimeContext.getTxn(), showStatementInfo);
            probeResultSet.executeScan(hash, ctx);
            populated=false;
            this.outerJoin = outerJoin;
            this.clone = cloneResults;
        }

				@Override
				public boolean hasNext() {
					if (LOG.isDebugEnabled())
							SpliceLogUtils.debug(LOG, ">>> NestdLoopJoin hasNext() ",(restriction != null?"with ":"without "),"restriction");
						if(populated)return true;
						rightResultSet.clearCurrentRow();
						try {
								ExecRow tmp = probeResultSet.getNextRowCore();
								ExecRow rightRow = (tmp != null ? tmp.getClone() : tmp);
                /*
                 * notExistsRightSide indicates that we need to reverse the logic of the join.
                 */
								if(notExistsRightSide){
										rightRow = (rightRow == null) ? getEmptyRow() : null;
								}
								if (outerJoin && rightRow == null){
									if (LOG.isDebugEnabled())
										SpliceLogUtils.debug(LOG, ">>> NestdLoopJoin: Outer join with empty right row");
										rightRow = getEmptyRightRow();
								}
								if(rightRow!=null){
										if(oneRowRightSide &&returnedRight){
												//skip this row, because it's already been found.
												returnedRight = false;
												populated=false;
												return false;
										}

										SpliceLogUtils.debug(LOG, ">>> NestdLoopJoin Right: ",rightRow);
					/*
					 * the right result set's row might be used in other branches up the stack which
					 * occur under serialization, so the activation has to be sure and set the current row
               * on rightResultSet, or that row won't be serialized over, potentially breaking ProjectRestricts
					 * up the stack.
					 */
										rightResultSet.setCurrentRow(rightRow); //set this here for serialization up the stack
										nonNullRight();
										returnedRight=true;

                                        if (clone) {
                                            mergedRow = mergedRow.getClone();
                                        }
										mergedRow = JoinUtils.getMergedRow(leftRow,rightRow,wasRightOuterJoin,rightNumCols,leftNumCols,mergedRow);
								}else {
										SpliceLogUtils.debug(LOG, ">>> NestdLoopJoin Right: ",rightRow);
										populated = false;
										return false;
								}

								if (restriction != null) {
										DataValueDescriptor restrictBoolean = restriction.invoke();
										if ((! restrictBoolean.isNull()) && restrictBoolean.getBoolean()) {
												SpliceLogUtils.trace(LOG, "restricted row %s",mergedRow);
												populated=false;
												hasNext();
										}
								}
						} catch (StandardException e) {
								try {
										close();
								} catch (StandardException e1) {
										SpliceLogUtils.logAndThrowRuntime(LOG, "close Failed", e1);
								}
								SpliceLogUtils.logAndThrowRuntime(LOG, "hasNext Failed", e);
								populated=false;
								return false;
						}
						populated=true;
						return true;
				}

				protected void nonNullRight() {
						//no op for inner loops
				}

				protected ExecRow getEmptyRightRow() throws StandardException {
						return null;  //for inner loops, return null here
				}

				@Override
				public ExecRow next() {
						SpliceLogUtils.trace(LOG, "next row=%s",mergedRow);
						populated=false;
						return mergedRow;
				}

				@Override
				public void remove() {
						SpliceLogUtils.trace(LOG, "remove");
				}
				public void close() throws StandardException {
						beginTime = getCurrentTimeMillis();
						if(shouldRecordStats()){
                            removeFromOperationChain();
                            //have to set a unique Task id each time to ensure all the rows are written
							probeResultSet.getDelegate().setTaskId(SpliceDriver.driver().getUUIDGenerator().nextUUID());
							//probeResultSet.getDelegate().setScrollId(Bytes.toLong(uniqueSequenceID));
							if(region!=null)
										probeResultSet.getDelegate().setRegionName(region.getRegionNameAsString());

                            IOStats stats = probeResultSet.getStats();
                            rowsSeenRight += stats.elementsSeen();
                            bytesReadRight += stats.bytesSeen();
                            TimeView timer = stats.getTime();
                            remoteScanCpuTime += timer.getCpuTime();
                            remoteScanUserTime += timer.getUserTime();
                            remoteScanWallTime += timer.getWallClockTime();
						}
						probeResultSet.close();
						closeTime += getElapsedMillis(beginTime);
				}

        @Override
        public Iterator<ExecRow> iterator() {
            return this;
        }
    }

    @Override
    public boolean providesRDD() {
        return leftResultSet.providesRDD();
    }

    @Override
    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {
        JavaRDD<LocatedRow> left = leftResultSet.getRDD(spliceRuntimeContext, leftResultSet);
        final SpliceObserverInstructions soi = SpliceObserverInstructions.create(activation, this, spliceRuntimeContext);
        return left.flatMap(new NLJSparkOperation(this, soi)).map(new Function<ExecRow, LocatedRow>() {
            @Override
            public LocatedRow call(ExecRow execRow) throws Exception {
                return new LocatedRow(execRow);
            }
        });
    }


    public static final class NLJSparkOperation extends SparkFlatMapOperation<NestedLoopJoinOperation, LocatedRow, ExecRow> {
        private NestedLoopIterator nestedLoopIterator;
        private byte[] rightResultSetUniqueSequenceID;

        public NLJSparkOperation() {
        }

        public NLJSparkOperation(NestedLoopJoinOperation spliceOperation, SpliceObserverInstructions soi) {
            super(spliceOperation, soi);
        }

        @Override
        public Iterable<ExecRow> call(LocatedRow sourceRow) throws Exception {
            if (sourceRow == null) {
                return null;
            }
            op.leftResultSet.setCurrentRow(sourceRow.getRow());
            if (rightResultSetUniqueSequenceID == null) {
                rightResultSetUniqueSequenceID = op.rightResultSet.getUniqueSequenceID();
            }
            nestedLoopIterator = op.createNestedLoopIterator(sourceRow.getRow(), op.isHash, soi.getSpliceRuntimeContext(), false, true);
            return nestedLoopIterator;
        }
    }
}
