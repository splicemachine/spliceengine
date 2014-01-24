package com.splicemachine.derby.metrics;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Lists;
import com.splicemachine.encoding.MultiFieldEncoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

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
}
