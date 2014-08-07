package com.splicemachine.derby.metrics;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.WriteStats;
import com.splicemachine.metrics.TimeView;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author Scott Fines
 * Date: 1/16/14
 */
public class OperationRuntimeStats {
		private final long statementId;
		private final long operationId;
		private final long taskId;
		private final String regionName; //can be null if there is no region

		//cached for performance
		private static final int maxSize = OperationMetric.values().length;

		private final ArrayList<OperationMetric> setMetrics;
		private final LongArrayList measuredValues;
		private String hostName;
		private double bufferFillRatio = 0d;

		public OperationRuntimeStats(long statementId,
																 long operationId,
																 long taskId,String regionName,int initialSize) {
				this.statementId = statementId;
				this.operationId = operationId;
				this.taskId = taskId;
				this.regionName = regionName;

				this.setMetrics = Lists.newArrayListWithExpectedSize(initialSize);
				this.measuredValues = new LongArrayList(initialSize);
				this.hostName = SpliceUtils.getHostName();
		}

		public void addMetric(OperationMetric metric, long value){
				//keep the array sorted according to position
				int position = metric.getPosition();
				for(int i=0;i<setMetrics.size();i++){
						if(setMetrics.get(i)==metric){
								measuredValues.set(i,measuredValues.get(i)+value);
								return;
						}else if(setMetrics.get(i).getPosition()>position){
								//we've reached a value that's higher than us, push it out an
								//entry
								setMetrics.add(i,metric);
								measuredValues.insert(i,value);
								return;
						}
				}
				setMetrics.add(metric);
				measuredValues.add(value);
		}

		public void encode(MultiFieldEncoder fieldEncoder) {
				fieldEncoder.encodeNext(statementId)
								.encodeNext(operationId)
								.encodeNext(taskId)
								.encodeNext(hostName);
				if(regionName!=null)
						fieldEncoder.encodeNext(regionName);
				else
					fieldEncoder.encodeEmpty();

				OperationMetric[] allMetrics = OperationMetric.values();
				Arrays.sort(allMetrics,new Comparator<OperationMetric>() {
						@Override
						public int compare(OperationMetric o1, OperationMetric o2) {
								return o1.getPosition()-o2.getPosition();
						}
				});
				int metricIndex=0;
				for(int storedMetricPos=0;storedMetricPos<setMetrics.size();storedMetricPos++){
						OperationMetric metric = setMetrics.get(storedMetricPos);
						while(metric.getPosition()>allMetrics[metricIndex].getPosition()){
								metricIndex++;
								fieldEncoder.encodeNext(0l); //assume that these fields are zero
						}
						fieldEncoder.encodeNext(measuredValues.get(storedMetricPos));
						metricIndex++;
				}
				while(metricIndex<allMetrics.length){
					fieldEncoder.encodeNext(0l);
						metricIndex++;
				}
				fieldEncoder.encodeNext(bufferFillRatio);
		}

		public long getStatementId() { return statementId; }
		public long getOperationId() { return operationId; }
		public long getTaskId() { return taskId; }

		public void setHostName(String hostName) {
				this.hostName = hostName;
		}

		public void setBufferFillRatio(double maxFillRatio) {
			this.bufferFillRatio = maxFillRatio;
		}

		public static List<OperationRuntimeStats> getOperationStats(SpliceOperation topOperation,
                                                                    long taskId,
                                                                    long statementId,
                                                                    WriteStats writeStats,
                                                                    TimeView writeTimer,
                                                                    SpliceRuntimeContext runtimeContext){
				List<OperationRuntimeStats> stats = Lists.newArrayList();
                OperationRuntimeStats metrics = getTopMetrics(topOperation, taskId, statementId, writeStats, writeTimer);
				if(metrics!=null){
						stats.add(metrics);
				}

                SpliceRuntimeContext.Side side = runtimeContext.getPathSide(topOperation.resultSetNumber());
                if (side == SpliceRuntimeContext.Side.LEFT || side == SpliceRuntimeContext.Side.MERGED) {
                    SpliceOperation child = topOperation.getLeftOperation();
                    if(child!=null)
                        populateStats(runtimeContext,child,statementId,taskId,stats);
                }

                if (side == SpliceRuntimeContext.Side.RIGHT || side == SpliceRuntimeContext.Side.MERGED) {
                    SpliceOperation child = topOperation.getRightOperation();
                    if(child!=null)
                        populateStats(runtimeContext,child,statementId,taskId,stats);
                }
				return stats;
		}

		private static OperationRuntimeStats getTopMetrics(SpliceOperation topOperation, long taskId, long statementId,
														   WriteStats writeStats, TimeView writeTimer) {
				OperationRuntimeStats metrics = topOperation.getMetrics(statementId,taskId, true);

				if(metrics!=null && writeStats.getRowsWritten()>=0){
						metrics.addMetric(OperationMetric.PROCESSING_CPU_TIME,writeTimer.getCpuTime());
						metrics.addMetric(OperationMetric.PROCESSING_USER_TIME,writeTimer.getUserTime());
						metrics.addMetric(OperationMetric.PROCESSING_WALL_TIME,writeTimer.getWallClockTime());


						addWriteStats(writeStats, metrics);
				}
				return metrics;
		}

		public static void addWriteStats(WriteStats writeStats, OperationRuntimeStats metrics) {
				metrics.addMetric(OperationMetric.WRITE_ROWS,writeStats.getRowsWritten());
				metrics.addMetric(OperationMetric.WRITE_BYTES,writeStats.getBytesWritten());
				metrics.addMetric(OperationMetric.REJECTED_WRITE_ATTEMPTS,writeStats.getRejectedCount());
				metrics.addMetric(OperationMetric.RETRIED_WRITE_ATTEMPTS,writeStats.getTotalRetries());
				metrics.addMetric(OperationMetric.FAILED_WRITE_ATTEMPTS,writeStats.getGlobalErrors());
				metrics.addMetric(OperationMetric.PARTIAL_WRITE_FAILURES,writeStats.getPartialFailureCount());

				TimeView networkTime = writeStats.getNetworkTime();
				metrics.addMetric(OperationMetric.WRITE_NETWORK_WALL_TIME,networkTime.getWallClockTime());
				metrics.addMetric(OperationMetric.WRITE_NETWORK_CPU_TIME,networkTime.getCpuTime());
				metrics.addMetric(OperationMetric.WRITE_NETWORK_USER_TIME,networkTime.getUserTime());

				TimeView sleepTime = writeStats.getSleepTime();
				metrics.addMetric(OperationMetric.WRITE_SLEEP_WALL_TIME,sleepTime.getWallClockTime());
				metrics.addMetric(OperationMetric.WRITE_SLEEP_CPU_TIME,sleepTime.getCpuTime());
				metrics.addMetric(OperationMetric.WRITE_SLEEP_USER_TIME,sleepTime.getUserTime());
				TimeView threadedTime = writeStats.getTotalTime();
				metrics.addMetric(OperationMetric.WRITE_TOTAL_WALL_TIME,threadedTime.getWallClockTime());
				metrics.addMetric(OperationMetric.WRITE_TOTAL_CPU_TIME,threadedTime.getCpuTime());
				metrics.addMetric(OperationMetric.WRITE_TOTAL_USER_TIME,threadedTime.getUserTime());
		}

		private static void populateStats(SpliceRuntimeContext context, SpliceOperation operation,
																			long statementId, long taskIdLong, List<OperationRuntimeStats> stats) {
				if(operation==null) return;
				OperationRuntimeStats metrics = operation.getMetrics(statementId, taskIdLong, false);
				if(metrics!=null)
						stats.add(metrics);
				if(operation instanceof SinkingOperation)
						return; //found the first sink, so return

                SpliceRuntimeContext.Side side = context.getPathSide(operation.resultSetNumber());
                if (side == SpliceRuntimeContext.Side.LEFT || side == SpliceRuntimeContext.Side.MERGED) {
                    SpliceOperation child = operation.getLeftOperation();
                    if(child!=null)
                        populateStats(context,child,statementId,taskIdLong,stats);
                }

                if (side == SpliceRuntimeContext.Side.RIGHT || side == SpliceRuntimeContext.Side.MERGED) {
                    SpliceOperation child = operation.getRightOperation();
                    if(child!=null)
                        populateStats(context,child,statementId,taskIdLong,stats);
                }
		}
}
