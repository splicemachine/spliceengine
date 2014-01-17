package com.splicemachine.derby.metrics;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Lists;
import com.splicemachine.encoding.MultiFieldEncoder;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Scott Fines
 * Date: 1/16/14
 */
public class OperationRuntimeStats {
		private final long statementId;
		private final long operationId;
		private final long taskId;

		//cached for performance
		private static final int maxSize = OperationMetric.values().length;

		private final ArrayList<OperationMetric> setMetrics;
		private final LongArrayList measuredValues;

		public OperationRuntimeStats(long statementId, long operationId, long taskId,int initialSize) {
				this.statementId = statementId;
				this.operationId = operationId;
				this.taskId = taskId;

				this.setMetrics = Lists.newArrayListWithExpectedSize(initialSize);
				this.measuredValues = new LongArrayList(initialSize);
		}

		public void addMetric(OperationMetric metric, long value){
				//keep the array sorted according to position
				int position = metric.getPosition();
				for(int i=0;i<setMetrics.size();i++){
						if(setMetrics.get(i)==metric){
								measuredValues.set(i,measuredValues.get(i)+value);
						}else if(setMetrics.get(i).getPosition()>position){
								//we've reached a value that's higher than us, push it out an
								//entry
								setMetrics.add(i,metric);
								measuredValues.insert(i,value);
						}
				}
		}

		public void encode(MultiFieldEncoder fieldEncoder) {
				fieldEncoder.encodeNext(statementId)
								.encodeNext(operationId)
								.encodeNext(taskId);

				OperationMetric[] allMetrics = OperationMetric.values();
				int metricIndex=0;
				for(int storedMetricPos=0;storedMetricPos<setMetrics.size();storedMetricPos++){
						OperationMetric metric = setMetrics.get(storedMetricPos);
						while(metric.getPosition()>allMetrics[metricIndex].getPosition()){
								metricIndex++;
								fieldEncoder.encodeNext(0l); //assume that these fields are zero
						}
						fieldEncoder.encodeNext(measuredValues.get(storedMetricPos));
				}
		}

		public long getStatementId() { return statementId; }
		public long getOperationId() { return operationId; }
		public long getTaskId() { return taskId; }
}
