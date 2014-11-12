package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.SingleScanRowProvider;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.job.JobResults;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;


/**
 *
 * @author Scott Fines
 *
 */
public abstract class DMLWriteOperation extends SpliceBaseOperation implements SinkingOperation {
		private static final long serialVersionUID = 2l;
		private static final Logger LOG = Logger.getLogger(DMLWriteOperation.class);
		protected SpliceOperation source;
		public SpliceOperation savedSource;
		protected long heapConglom;
		protected DataDictionary dd;
		protected TableDescriptor td;

		protected static List<NodeType> parallelNodeTypes = Arrays.asList(NodeType.REDUCE,NodeType.SCAN);
		protected static List<NodeType> sequentialNodeTypes = Arrays.asList(NodeType.SCAN);
		private boolean isScan = true;
		private ModifiedRowProvider modifiedProvider;

		protected DMLWriteInfo writeInfo;

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
		public List<NodeType> getNodeTypes() {
				return isScan ? parallelNodeTypes : sequentialNodeTypes;
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
				SpliceLogUtils.trace(LOG, "init with regionScanner %s", regionScanner);
				super.init(context);
				source.init(context);
				writeInfo.initialize(context);

				List<SpliceOperation> opStack = getOperationStack();
				boolean hasScan = false;
				for(SpliceOperation op:opStack){
						if(this!=op&&op.getNodeTypes().contains(NodeType.REDUCE)||op instanceof ScanOperation){
								hasScan =true;
								break;
						}
				}
				isScan = hasScan;
				startExecutionTime = System.currentTimeMillis();
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
		public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				SpliceLogUtils.trace(LOG,"executeScan");
		/*
		 * Write the data from the source sequentially. 
		 * We make a distinction here, because Inserts either happen in
		 * parallel or sequentially, but never both; Thus, if we have a Reduce
		 * nodetype, this should be executed in parallel, so *don't* attempt to
		 * insert here.
		 */
				try {
						RowProvider rowProvider = getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext),runtimeContext);
						modifiedProvider = new ModifiedRowProvider(rowProvider,writeInfo.buildInstructions(this));
						//modifiedProvider.setRowsModified(rowsSunk);
						return new SpliceNoPutResultSet(activation,this,modifiedProvider,false);
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				return source.getMapRowProvider(top, decoder, spliceRuntimeContext);
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				return source.getReduceRowProvider(top, decoder, spliceRuntimeContext,returnDefaultValue);
		}

		@Override
		protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {
        long start = System.currentTimeMillis();
        final RowProvider rowProvider = getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext),runtimeContext);

        nextTime+= System.currentTimeMillis()-start;

        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(), this, runtimeContext);
        jobResults = rowProvider.shuffleRows(soi,OperationUtils.cleanupSubTasks(this));

				long rowsModified = 0;
				for(TaskStats stats:jobResults.getJobStats().getTaskStats()){
						rowsModified+=stats.getTotalRowsWritten();
				}
				this.rowsSunk = rowsModified;
				return jobResults;
		}

		@Override
		public void open() throws StandardException, IOException {
				SpliceLogUtils.trace(LOG,"Open");
				super.open();
				if(source!=null)source.open();
		}

		@Override
		public void close() throws StandardException, IOException {
				super.close();
				source.close();
				if (modifiedProvider != null)
						modifiedProvider.close();
		}

		@Override
		public byte[] getUniqueSequenceId() {
				return uniqueSequenceID;
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
//				ExecRow row = source.getExecRowDefinition();
				SpliceLogUtils.trace(LOG,"execRowDefinition=%s",row);
				return row;
		}

		public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(timer==null){
						timer = spliceRuntimeContext.newTimer();
				}

				timer.startTiming();
				ExecRow row = source.nextRow(spliceRuntimeContext);
				if(row!=null){
						timer.tick(1);
						currentRow = row;
				}else{
						timer.stopTiming();
						stopExecutionTime = System.currentTimeMillis();
				}
				return row;
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				throw new UnsupportedOperationException("Write Operations do not produce rows.");
		}

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
            if (timer != null) {
                //inputs rows are the same as output rows by default (although Update may be different)
                stats.addMetric(OperationMetric.INPUT_ROWS, timer.getNumEvents());
            }
		}

		private class ModifiedRowProvider extends SingleScanRowProvider{
				private volatile boolean isOpen;
				private long rowsModified=0;
				private RowProvider rowProvider;
				private SpliceObserverInstructions spliceObserverInstructions;

				@Override
				public JobResults shuffleRows(SpliceObserverInstructions instructions,
                                      Callable<Void>... postCompleteTasks) throws StandardException, IOException {
            /*
             * Transactional Information:
             *
             * When we execute a statement like this, we have a situation. Our root
             * transaction is the transaction for the overall transaction (not a
             * statement specific one). This means that, if anything happens here,
             * we need to rollback the overall transaction.
             *
             * However, there are two problems with this. Firstly, if auto-commit
             * is off, then a user may choose to commit that global transaction
             * no matter what we do, which could result in data corruption. Secondly,
             * if we automatically rollback the global transaction, then we risk
             * rolling back many statements which were executed in parallel. This
             * is clearly very bad in both cases, so we can't rollback the
             * global transaction.
             *
             * Instead, we create a child transaction, which we can roll back
             * safely.
             *
             * -sf- In past versions of Splice, we would create the child transaction
             * directly right here, and then manually commit/rollback as needed. However,
             * DB-1706 implements Derby savepoints, which the derby query parser automatically
             * creates during any write operation. Thus, we no longer have to explicitly
             * create or manage transactions here, as it is transparently managed
             * by Derby for us.
             */
						JobResults jobStats = rowProvider.shuffleRows(instructions,postCompleteTasks);
						long i = 0;
						for (TaskStats stat: jobStats.getJobStats().getTaskStats()) {
								i = i + stat.getTotalRowsWritten();
						}
						rowsModified = i;
						return jobStats;
				}

				public ModifiedRowProvider(RowProvider rowProvider, SpliceObserverInstructions spliceObserverInstructions) {
						this.rowProvider = rowProvider;
						this.spliceObserverInstructions = spliceObserverInstructions;
				}

				@Override public boolean hasNext() { return false; }
				@Override public ExecRow next() { return null; }

				public void setRowsModified(long rowsModified){
						this.isOpen = true;
						this.rowsModified = rowsModified;
				}

				//no-op
				@Override public void reportStats(long statementId, long operationId, long taskId, String xplainSchema,String regionName) {  }

				@Override
				public IOStats getIOStats() {
						return Metrics.noOpIOStats();
				}

				@Override
				public void open() throws StandardException {
						SpliceLogUtils.trace(LOG, "open");
						this.isOpen = true;

            if(!getNodeTypes().contains(NodeType.REDUCE)){
                if (rowProvider != null)
                    rowProvider.open();
                else if(operationInformation.isRuntimeStatisticsEnabled()) {
										/* Cache query plan text for source, before it gets blown away */
										/* savedSource nulled after run time statistics generation */
                    savedSource = source;
                }
                //delegate to shuffle, since that will do the proper thing
                try {
                    shuffleRows(spliceObserverInstructions);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
            }
				}

        @Override
				public void close() throws StandardException{
						rowsModified = 0;
						SpliceLogUtils.trace(LOG, "close in modifiedProvider for Delete/Insert/Update");
						if (! this.isOpen)
								return;
						if (isTopResultSet && activation.getLanguageConnectionContext().getRunTimeStatisticsMode() &&
										!activation.getLanguageConnectionContext().getStatementContext().getStatementWasInvalidated())
								endExecutionTime = getCurrentTimeMillis();

						this.isOpen = false;
						try {
								if (rowProvider != null)
										rowProvider.close();
								else
										source.close();
						} catch (IOException e) {
                throw Exceptions.parseException(e);
						}
				}

				@Override public RowLocation getCurrentRowLocation() { return null; }
				@Override public Scan toScan() { return null; }
				@Override public byte[] getTableName() { return null; }

				@Override
				public int getModifiedRowCount() {
						return (int)(rowsModified+rowsSunk);
				}

				@Override
				public String toString(){
						return "ModifiedRowProvider";
				}

				@Override
				public SpliceRuntimeContext getSpliceRuntimeContext() {
						return spliceRuntimeContext;
				}
		}

		@Override
		public int modifiedRowCount() {
				return modifiedProvider.getModifiedRowCount();
		}

		public SpliceOperation getSource() {
				return this.source;
		}

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

    Txn getChildTransaction() {
        byte[] destTable = Bytes.toBytes(Long.toString(heapConglom));
        TxnView parentTxn = operationInformation.getTransaction();
        try{
            return TransactionLifecycle.getLifecycleManager().beginChildTransaction(parentTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION, false,destTable);
        }catch(IOException ioe){
            LOG.error(ioe);
            throw new RuntimeException(ErrorState.XACT_INTERNAL_TRANSACTION_EXCEPTION.newException());
        }
		}
}
