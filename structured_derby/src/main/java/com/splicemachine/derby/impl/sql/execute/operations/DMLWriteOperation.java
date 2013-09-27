package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.SingleScanRowProvider;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.job.JobStats;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


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
	ConstantAction constants;
	protected long heapConglom;
	
	protected DataDictionary dd;
	protected TableDescriptor td;
	
	protected boolean autoIncrementGenerated;
	
	protected static List<NodeType> parallelNodeTypes = Arrays.asList(NodeType.REDUCE,NodeType.SCAN);
	protected static List<NodeType> sequentialNodeTypes = Arrays.asList(NodeType.SCAN);
	
	private boolean isScan = true;

    protected FormatableBitSet pkColumns;
    protected int[] pkCols;
	
	public DMLWriteOperation(){
		super();
	}
	
	public DMLWriteOperation(Activation activation) throws StandardException{
		super(activation,-1,0d,0d);
		this.activation = activation;
		init(SpliceOperationContext.newContext(activation));
	}
	
	public DMLWriteOperation(SpliceOperation source, Activation activation) throws StandardException{
		super(activation,-1,0d,0d);
		this.source = source;
		this.activation = activation;
		init(SpliceOperationContext.newContext(activation));
	}
	
	public DMLWriteOperation(SpliceOperation source,
			GeneratedMethod generationClauses, 
			GeneratedMethod checkGM) throws StandardException{
		super(source.getActivation(),-1,0d,0d);
		this.source = source;
		this.activation = source.getActivation();
		init(SpliceOperationContext.newContext(activation));
	}
	
	public DMLWriteOperation(SpliceOperation source,
							GeneratedMethod generationClauses, 
							GeneratedMethod checkGM,
							Activation activation) throws StandardException{
		super(activation,-1,0d,0d);
		this.source = source;
		this.activation = activation;
        init(SpliceOperationContext.newContext(activation));
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
        if(in.readBoolean())
            pkColumns = (FormatableBitSet)in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(source);
        out.writeBoolean(pkColumns!=null);
        if(pkColumns!=null){
            out.writeObject(pkColumns);
        }
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG,"init with regionScanner %s",regionScanner);
		super.init(context);
		((SpliceOperation)source).init(context);
		
		constants = activation.getConstantAction();

		List<SpliceOperation> opStack = getOperationStack();
		boolean hasScan = false;
		for(SpliceOperation op:opStack){
			if(this!=op&&op.getNodeTypes().contains(NodeType.REDUCE)||op instanceof ScanOperation){
				hasScan =true;
				break;
			}
		}
		isScan = hasScan;
	}

    public byte[] getDestinationTable(){
        return Long.toString(heapConglom).getBytes();
    }

	@Override
	public SpliceOperation getLeftOperation() {
		return (SpliceOperation)source;
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		return Collections.singletonList((SpliceOperation)source);
	}

	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG,"executeScan");
		/*
		 * Write the data from the source sequentially. 
		 * We make a distinction here, because Inserts either happen in
		 * parallel or sequentially, but never both; Thus, if we have a Reduce
		 * nodetype, this should be executed in parallel, so *don't* attempt to
		 * insert here.
		 */
		return new SpliceNoPutResultSet(activation,this,modifiedProvider,false);
	}

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder decoder) throws StandardException {
        return ((SpliceOperation)source).getMapRowProvider(top, decoder);
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, RowDecoder decoder) throws StandardException {
        return ((SpliceOperation)source).getReduceRowProvider(top, decoder);
    }

    @Override
    protected JobStats doShuffle() throws StandardException {
        JobStats jobStats = super.doShuffle();

        Map<String,TaskStats> taskStats = jobStats.getTaskStats();
        long rowsModified = 0;
        for(String taskId:taskStats.keySet()){
            TaskStats stats = taskStats.get(taskId);
            rowsModified+=stats.getWriteStats().getTotalRecords();
        }
        modifiedProvider.addRowsModified(rowsModified);
        return jobStats;
    }

    @Override
    public void open() throws StandardException, IOException {
        SpliceLogUtils.trace(LOG,"Open");
        super.open();
        if(source!=null)source.open();
    }

    @Override
    public void close() throws StandardException, IOException {
        modifiedProvider.close();
        super.close();
    }

    @Override
	public ExecRow getExecRowDefinition() throws StandardException {
		ExecRow row = ((SpliceOperation)source).getExecRowDefinition();
		SpliceLogUtils.trace(LOG,"execRowDefinition=%s",row);
		return row;
	}

	public ExecRow getNextSinkRow() throws StandardException, IOException {
        return source.nextRow();
	}

	public ExecRow nextRow() throws StandardException {
        throw new UnsupportedOperationException("Write Operations do not produce rows.");
	}

    protected FormatableBitSet fromIntArray(int[] values){
        if(values ==null) return null;
        FormatableBitSet fbt = new FormatableBitSet(values.length);
        for(int value:values){
            fbt.grow(value);
            fbt.set(value-1);
        }
        return fbt;
    }

    private final ModifiedRowProvider modifiedProvider = new ModifiedRowProvider();

    private class ModifiedRowProvider extends SingleScanRowProvider{
        private volatile boolean isOpen;
		private long rowsModified=0;
		@Override public boolean hasNext() { return false; }

		@Override public ExecRow next() { return null; }

        public void setRowsModified(long rowsModified){
            this.isOpen = true;
            this.rowsModified = rowsModified;
        }

        public void addRowsModified(long rowsModified){
            this.isOpen = true;
            this.rowsModified += rowsModified;
        }

		@Override
		public void open()  {
			SpliceLogUtils.trace(LOG, "open");
            this.isOpen = true;
			try {
				source.open();
				/* Cache query plan text for source, before it gets blown away */
				if (activation.getLanguageConnectionContext().getRunTimeStatisticsMode())
				{
					/* savedSource nulled after run time statistics generation */
					savedSource = source;
				}
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG, e);
			} catch (IOException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG,e);
            }
            if(!getNodeTypes().contains(NodeType.REDUCE)){
                /*
                 * We are executing the operation directly, because there's no need
                 * to submit a parallel task
                 *
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
                 */
                TransactionId parentTxnId = HTransactorFactory.getTransactor().transactionIdFromString(getTransactionID());
                TransactionId childTransactionId;
                try{
                    childTransactionId = HTransactorFactory.getTransactor().beginChildTransaction(parentTxnId,true);
                }catch(IOException ioe){
                    LOG.error(ioe);
                    throw new RuntimeException(ErrorState.XACT_INTERNAL_TRANSACTION_EXCEPTION.newException());
                }

                try {
                    OperationSink opSink = OperationSink.create(DMLWriteOperation.this, null, childTransactionId.getTransactionIdString());
                    TaskStats stats = opSink.sink(getDestinationTable());
                    modifiedProvider.setRowsModified(stats.getReadStats().getTotalRecords());

                    //commit the transaction
                    commitTransactionSafely(childTransactionId,0);
                } catch(Exception se){
                    if(se instanceof StandardException && ErrorState.XACT_COMMIT_EXCEPTION.getSqlState().equals(((StandardException)se).getSqlState())){
                        //bad news, we couldn't commit the transaction. Nothing we can do there,
                        //just propagate
                        throw new RuntimeException(se);
                    }
                    //we encountered some other kind of error. Try and roll back the Transaction
                    try {
                        rollback(childTransactionId,0);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                    throw new RuntimeException(se);
                } finally {
                    clearChildTransactionID();
                }
			}
		}

        private void rollback(TransactionId txnId, int numTries) throws StandardException {
            if(numTries> SpliceConstants.numRetries)
                throw ErrorState.XACT_ROLLBACK_EXCEPTION.newException();
            try{
                HTransactorFactory.getTransactor().rollback(txnId);
            }catch (IOException e) {
                TransactionStatus status = getTransactionStatusSafely(txnId,0);
                switch (status) {
                    case ACTIVE:
                        //we can retry
                        rollback(txnId, numTries+1);
                        return;
                    case COMMITTING:
                        throw new IllegalStateException("Seen a transaction state of COMMITTING");
                    case ROLLED_BACK:
                        //whoo, we succeeded
                        return;
                    default:
                        throw ErrorState.XACT_ROLLBACK_EXCEPTION.newException();
                }
            }
        }

        private void commitTransactionSafely(TransactionId txnId, int numTries) throws StandardException{
            /*
             * We attempt to commit the transaction safely.
             *
             * If we fail to commit, then we must check our transaction state, and retry commit if necessary
             */
            //we're out of tries, give up
            if(numTries> SpliceConstants.numRetries)
                throw ErrorState.XACT_COMMIT_EXCEPTION.newException();

            try{
                HTransactorFactory.getTransactor().commit(txnId);
            } catch (IOException e) {
                TransactionStatus status = getTransactionStatusSafely(txnId,0);
                switch (status) {
                    case COMMITTED:
                        //whoo, success!
                        return;
                    case ACTIVE:
                        //cool, we can retry the commit
                        commitTransactionSafely(txnId, numTries+1);
                        return;
                    case COMMITTING:
                        throw new IllegalStateException("Seen a committing transaction state!");
                    default:
                        //we can't retry, that's bad
                        throw ErrorState.XACT_COMMIT_EXCEPTION.newException();
                }
            }
        }

        private TransactionStatus getTransactionStatusSafely(TransactionId txnId, int numTries) throws StandardException{
            if(numTries> SpliceConstants.numRetries)
                throw ErrorState.XACT_COMMIT_EXCEPTION.newException();
            try{
                return HTransactorFactory.getTransactor().getTransactionStatus(txnId);
            }catch(IOException ioe){
                LOG.error(ioe);
                //try again
                return getTransactionStatusSafely(txnId, numTries+1);
            }
        }

        @Override
		public void close() {
			SpliceLogUtils.trace(LOG, "close in modifiedProvider for Delete/Insert/Update");
            if (! this.isOpen)
				return;
			if (isTopResultSet && activation.getLanguageConnectionContext().getRunTimeStatisticsMode() &&
                    !activation.getLanguageConnectionContext().getStatementContext().getStatementWasInvalidated())
				endExecutionTime = getCurrentTimeMillis();

            this.rowsModified = 0;
            this.isOpen = false;
            try {
				source.close();
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG, e);
			} catch (IOException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG, e);
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
	};
	
	@Override
	public int modifiedRowCount() {
		if (modifiedProvider != null)
			return modifiedProvider.getModifiedRowCount();
		else
			return (int)rowsSunk;
	}
	
	public SpliceOperation getSource() {
		return this.source;
	}

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);

        return new StringBuilder()
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("heapConglom:").append(heapConglom)
                .append(indent).append("isScan:").append(isScan)
                .append(indent).append("pkColumns:").append(pkColumns)
                .append(indent).append("source:").append(((SpliceOperation)source).prettyPrint(indentLevel+1))
                .toString();
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
        return ((SpliceOperation)source).getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return ((SpliceOperation)source).isReferencingTable(tableNumber);
    }
}
