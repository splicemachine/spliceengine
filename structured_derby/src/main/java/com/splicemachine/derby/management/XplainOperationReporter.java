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
								BitSet fields  = new BitSet(8);
								fields.set(0,8);
								BitSet scalarFields = new BitSet(8);
								scalarFields.set(0, 2);
								scalarFields.set(3);
								scalarFields.set(6,8);
								BitSet floatFields = new BitSet(0);
								BitSet doubleFields = new BitSet(0);

								return EntryEncoder.create(SpliceDriver.getKryoPool(),8,fields,scalarFields,floatFields,doubleFields);
						}

						@Override
						protected void doEncode(MultiFieldEncoder encoder, OperationInfo element) {
								long parentOperationUuid = element.getParentOperationUuid();
								encoder.encodeNext(element.getStatementId())
												.encodeNext(element.getOperationUuid())
												.encodeNext(element.getOperationTypeName());
								if(parentOperationUuid==-1l)
										encoder.encodeEmpty();
								else
										encoder.encodeNext(parentOperationUuid);

								encoder.encodeNext(element.isRight())
												.encodeNext(element.getNumJobs() > 0)
												.encodeNext(element.getNumJobs())
												.encodeNext(element.getNumTasks());
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
