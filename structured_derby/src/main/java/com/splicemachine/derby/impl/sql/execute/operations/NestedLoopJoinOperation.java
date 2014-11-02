package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
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
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
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
						nestedLoopIterator = new NestedLoopIterator(leftRow, isHash, outerJoin, spliceRuntimeContext);
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
            stats.addMetric(OperationMetric.OUTPUT_ROWS,timer.getNumEvents());
            stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,rowsSeenRight);
            stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES, bytesReadRight);
            stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME, remoteScanWallTime);
            stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME, remoteScanCpuTime);
            stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME, remoteScanUserTime);
            super.updateStats(stats);
		}

    protected class NestedLoopIterator implements Iterator<ExecRow> {
        protected ExecRow leftRow;
        protected OperationResultSet probeResultSet;
        private boolean populated;
        private boolean returnedRight=false;
        private boolean outerJoin = false;


        NestedLoopIterator(ExecRow leftRow, boolean hash, boolean outerJoin, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
            SpliceLogUtils.trace(LOG, "NestedLoopIterator instantiated with leftRow %s",leftRow);
            this.leftRow = leftRow;
            probeResultSet = getRightResultSet();
            if (shouldRecordStats()) {
                if (operationChainInfo == null) {
                    operationChainInfo = new XplainOperationChainInfo(
                            spliceRuntimeContext.getStatementId(),
                            Bytes.toLong(uniqueSequenceID));
                }
                List<XplainOperationChainInfo> operationChain = SpliceBaseOperation.operationChain.get();
                if (operationChain == null) {
                    operationChain = Lists.newLinkedList();
                    SpliceBaseOperation.operationChain.set(operationChain);
                }
                operationChain.add(operationChainInfo);
            }
            SpliceRuntimeContext ctxNoSink = spliceRuntimeContext.copy();
            ctxNoSink.unMarkAsSink();
            probeResultSet.setParentOperationID(Bytes.toLong(getUniqueSequenceID()));
            SpliceRuntimeContext ctx = probeResultSet.sinkOpen(spliceRuntimeContext.getTxn(), true);
            probeResultSet.executeScan(hash,ctx);
            populated=false;
            this.outerJoin = outerJoin;
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
                            List<XplainOperationChainInfo> operationChain = SpliceBaseOperation.operationChain.get();
                            if (operationChain != null && operationChain.size() > 0) {
                                operationChain.remove(operationChain.size() - 1);
                            }
							//have to set a unique Task id each time to ensure all the rows are written
							probeResultSet.getDelegate().setTaskId(SpliceDriver.driver().getUUIDGenerator().nextUUID());
							//probeResultSet.getDelegate().setScrollId(Bytes.toLong(uniqueSequenceID));
							if(region!=null)
										probeResultSet.getDelegate().setRegionName(region.getRegionNameAsString());

                            IOStats stats = probeResultSet.getStats();
                            rowsSeenRight += stats.getRows();
                            bytesReadRight += stats.getBytes();
                            TimeView timer = stats.getTime();
                            remoteScanCpuTime += timer.getCpuTime();
                            remoteScanUserTime += timer.getUserTime();
                            remoteScanWallTime += timer.getWallClockTime();
						}
						probeResultSet.close();
						closeTime += getElapsedMillis(beginTime);
				}
		}
}
