package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 *
 * @author Scott Fines
 *
 */
public abstract class DMLWriteOperation extends SpliceBaseOperation {
		private static final long serialVersionUID = 2l;
		private static final Logger LOG = Logger.getLogger(DMLWriteOperation.class);
		protected SpliceOperation source;
		public SpliceOperation savedSource;
		protected long heapConglom;
		protected DataDictionary dd;
		protected TableDescriptor td;
		private boolean isScan = true;
		protected DMLWriteInfo writeInfo;
        protected long writeRowsFiltered;
		public DMLWriteOperation(){
				super();
		}

		public DMLWriteOperation(SpliceOperation source, Activation activation) throws StandardException{
				super(activation,-1,0d,0d);
				this.source = source;
				this.activation = activation;
				this.writeInfo = new DerbyDMLWriteInfo();
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}

		}

		public DMLWriteOperation(SpliceOperation source,
														 GeneratedMethod generationClauses,
														 GeneratedMethod checkGM,
														 Activation activation) throws StandardException{
				this(source,activation);
		}

		DMLWriteOperation(SpliceOperation source,
											OperationInformation opInfo,
											DMLWriteInfo writeInfo) throws StandardException{
				super(opInfo);
				this.writeInfo = writeInfo;
				this.source = source;
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,
						ClassNotFoundException {
				super.readExternal(in);
				source = (SpliceOperation)in.readObject();
				writeInfo = (DMLWriteInfo)in.readObject();
                heapConglom = in.readLong();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeObject(source);
				out.writeObject(writeInfo);
                out.writeLong(heapConglom);
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "DMLWriteOperation#init");
				super.init(context);
				source.init(context);
				writeInfo.initialize(context);
		}

		public byte[] getDestinationTable(){
				return Long.toString(heapConglom).getBytes();
		}

		@Override
		public SpliceOperation getLeftOperation() {
				return source;
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				return Collections.singletonList(source);
		}

		@Override
		public ExecRow getExecRowDefinition() throws StandardException {
				/*
				 * Typically, we just call down to our source and then pass that along
				 * unfortunately, with autoincrement columns this can lead to a
				 * StackOverflow, so we can't do that(see DB-1098 for more info)
				 *
				 * Luckily, DML operations are the top of their stack, so we can
				 * just form our exec row from our result description.
				 */
				ResultDescription description = writeInfo.getResultDescription();
				ResultColumnDescriptor[] rcd = description.getColumnInfo();
				DataValueDescriptor[] dvds = new DataValueDescriptor[rcd.length];
				for(int i=0;i<rcd.length;i++){
						dvds[i] = rcd[i].getType().getNull();
				}
				ExecRow row = new ValueRow(dvds.length);
				row.setRowArray(dvds);
				SpliceLogUtils.trace(LOG,"execRowDefinition=%s",row);
				return row;
		}

        abstract KeyEncoder getKeyEncoder() throws StandardException;

        abstract DataHash getRowHash() throws StandardException;

    @Override
		public String prettyPrint(int indentLevel) {
				String indent = "\n"+ Strings.repeat("\t",indentLevel);

				return indent + "resultSetNumber:" + resultSetNumber + indent
								+ "heapConglom:" + heapConglom + indent
								+ "isScan:" + isScan + indent
								+ "writeInfo:" + writeInfo + indent
								+ "source:" + source.prettyPrint(indentLevel + 1);
		}

		@Override
		public int[] getRootAccessedCols(long tableNumber) throws StandardException{
				return source.getRootAccessedCols(tableNumber);
		}

		@Override
		public boolean isReferencingTable(long tableNumber) {
				return source.isReferencingTable(tableNumber);
		}


    /**
     * Gets the number of rows that are "filtered". These are rows that derby thinks should have been
     * changed, but that we don't change due to some kind of optimization (e.g. not writing data that we know
     * will fail for some reason). This allows us to generate the "correct" number of rows modified without
     * actually physically writing it.
     *
     * The main usage of this is to enable UpdateOperations to work correctly while still emitting the
     * same number of rows as we expect(see DB-2007 for more information).
     *
     * @return the number of "filtered" rows
     */
    public long getFilteredRows(){
        return writeRowsFiltered;

    }

    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        DataSet set = source.getDataSet();
        return set.mapPartitions(new DMLWriteSparkOp(dsp.createOperationContext(this)));
    }

	public static final class DMLWriteSparkOp extends SpliceFlatMapFunction<SpliceOperation, Iterator<LocatedRow>, LocatedRow> {
		public DMLWriteSparkOp() {
		}

		public DMLWriteSparkOp(OperationContext<SpliceOperation> operationContext) {
			super(operationContext);
		}

        @Override
		public Iterable<LocatedRow> call(Iterator<LocatedRow> locatedRowIterator) throws Exception {
            DMLWriteOperation op = (DMLWriteOperation) getOperation();
			KeyEncoder keyEncoder = op.getKeyEncoder();
			DataHash rowHash = op.getRowHash();
			KVPair.Type dataType = op instanceof UpdateOperation ? KVPair.Type.UPDATE : KVPair.Type.INSERT;
			dataType = op instanceof DeleteOperation ? KVPair.Type.DELETE : dataType;
			TxnView txn = operationContext.getTxn();
			WriteCoordinator writeCoordinator = SpliceDriver.driver().getTableWriter();
            operationContext.recordWrite();
            // Add metrics?
			RecordingCallBuffer<KVPair> bufferToTransform = writeCoordinator.writeBuffer(op.getDestinationTable(), txn, Metrics.noOpMetricFactory());
			RecordingCallBuffer<KVPair> writeBuffer = op.transformWriteBuffer(bufferToTransform);
			PairEncoder encoder = new PairEncoder(keyEncoder, rowHash, dataType);
            int i = 0;
			while(locatedRowIterator.hasNext()) {
                LocatedRow lr = locatedRowIterator.next();
				ExecRow row = lr.getRow();
                operationContext.getOperation().setCurrentLocatedRow(lr);
				if (row == null) continue;
				KVPair encode = encoder.encode(row);
				writeBuffer.add(encode);
                i++;
			}
			writeBuffer.flushBuffer();
			writeBuffer.close();
            ValueRow valueRow = new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger(i));
			return Collections.singletonList(new LocatedRow(valueRow));
		}
	}
}
