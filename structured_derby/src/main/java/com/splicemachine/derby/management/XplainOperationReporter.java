package com.splicemachine.derby.management;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.EntryEncoder;


/**
 * @author Scott Fines
 *         Date: 1/22/14
 */
public class XplainOperationReporter extends XplainReporter<OperationInfo> {
		public XplainOperationReporter(int numWorkers) {
				super("SYSXPLAIN_OPERATIONHISTORY",numWorkers);
		}

		@Override
		protected DataHash<OperationInfo> getDataHash() {
				return new EntryWriteableHash<OperationInfo>() {
						@Override
						protected EntryEncoder buildEncoder() {
								int totalLength = 10;
								BitSet fields  = new BitSet(totalLength);
								fields.set(0,totalLength);
								BitSet scalarFields = new BitSet(totalLength);
								scalarFields.set(0, 2);
								scalarFields.set(3);
								scalarFields.set(7,totalLength);
								BitSet floatFields = new BitSet(0);
								BitSet doubleFields = new BitSet(0);

								return EntryEncoder.create(SpliceDriver.getKryoPool(),totalLength,fields,scalarFields,floatFields,doubleFields);
						}

						@Override
						protected void doEncode(MultiFieldEncoder encoder, OperationInfo element) {
								long parentOperationUuid = element.getParentOperationUuid();
                                String info = element.getInfo();
								encoder.encodeNext(element.getStatementId())
												.encodeNext(element.getOperationUuid())
												.encodeNext(element.getOperationTypeName());
								if(parentOperationUuid==-1l)
										encoder.encodeEmpty();
								else
										encoder.encodeNext(parentOperationUuid);

								encoder.encodeNext(info!=null?info:"")
                                        .encodeNext(element.isRight())
                                        .encodeNext(element.getNumJobs() > 0)
                                        .encodeNext(element.getNumJobs())
                                        .encodeNext(element.getNumTasks())
                                        .encodeNext(element.getNumFailedTasks());
						}
				};
		}

		@Override
		protected DataHash<OperationInfo> getKeyHash() {
				return new KeyWriteableHash<OperationInfo>() {
						@Override
						protected int getNumFields() {
								return 2;
						}

						@Override
						protected void doEncode(MultiFieldEncoder encoder, OperationInfo element) {
								encoder.encodeNext(element.getStatementId())
												.encodeNext(element.getOperationUuid());
						}
				};
		}


}
