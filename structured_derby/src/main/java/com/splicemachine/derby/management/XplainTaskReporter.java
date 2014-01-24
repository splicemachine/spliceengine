package com.splicemachine.derby.management;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.EntryEncoder;

import java.util.Arrays;
import java.util.Comparator;

/**
 * @author Scott Fines
 * Date: 1/22/14
 */
public class XplainTaskReporter extends XplainReporter<OperationRuntimeStats>{
		public XplainTaskReporter(int numWorkers) {
				super("SYSXPLAIN_TASKHISTORY", numWorkers);
		}

		@Override
		protected DataHash<OperationRuntimeStats> getDataHash() {
				return new EntryWriteableHash<OperationRuntimeStats>() {
						@Override
						protected EntryEncoder buildEncoder() {
								OperationMetric[] metrics = OperationMetric.values();
								Arrays.sort(metrics,new Comparator<OperationMetric>() {
										@Override
										public int compare(OperationMetric o1, OperationMetric o2) {
												return o1.getPosition()-o2.getPosition();
										}
								});
								int totalLength = metrics.length + 6;
								BitSet fields = new BitSet(totalLength);
								fields.set(0, totalLength);
								BitSet scalarFields = new BitSet(totalLength);
								scalarFields.set(0, 3);
								scalarFields.set(5, totalLength-1);
								BitSet floatFields = new BitSet(0);
								BitSet doubleFields = new BitSet(totalLength);
								doubleFields.set(totalLength-1);

								return EntryEncoder.create(SpliceDriver.getKryoPool(),totalLength,fields,scalarFields,floatFields,doubleFields);
						}

						@Override
						protected void doEncode(MultiFieldEncoder encoder, OperationRuntimeStats element) {
								element.encode(encoder);
						}
				};
		}

		@Override
		protected DataHash<OperationRuntimeStats> getKeyHash() {
				return new KeyWriteableHash<OperationRuntimeStats>() {
						@Override
						protected int getNumFields() {
								return 3;
						}

						@Override
						protected void doEncode(MultiFieldEncoder encoder, OperationRuntimeStats element) {
							encoder.encodeNext(element.getStatementId())
											.encodeNext(element.getOperationId())
											.encodeNext(element.getTaskId());
						}
				};
		}
}
