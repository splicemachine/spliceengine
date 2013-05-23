package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.SingleScanRowProvider;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.JobStatsUtils;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.TransactorFactoryImpl;
import com.splicemachine.utils.SpliceLogUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;


/**
 * 
 * @author Scott Fines
 *
 */
public abstract class DMLWriteOperation extends SpliceBaseOperation {
    private static final long serialVersionUID = 2l;
	private static final Logger LOG = Logger.getLogger(DMLWriteOperation.class);
	protected NoPutResultSet source;
	public NoPutResultSet savedSource;
	ConstantAction constants;
	protected long heapConglom;
	
	protected DataDictionary dd;
	protected TableDescriptor td;
	
	protected boolean autoIncrementGenerated;
	
	protected static List<NodeType> parallelNodeTypes = Arrays.asList(NodeType.REDUCE,NodeType.SCAN);
	protected static List<NodeType> sequentialNodeTypes = Arrays.asList(NodeType.SCAN);
	
	private boolean isScan = true;

    protected FormatableBitSet pkColumns;
	
	public DMLWriteOperation(){
		super();
	}
	
	public DMLWriteOperation(Activation activation) throws StandardException{
		super(activation,-1,0d,0d);
		this.activation = activation;
		init(SpliceOperationContext.newContext(activation));
	}
	
	public DMLWriteOperation(NoPutResultSet source, Activation activation) throws StandardException{
		super(activation,-1,0d,0d);
		this.source = source;
		this.activation = activation;
		init(SpliceOperationContext.newContext(activation));
	}
	
	public DMLWriteOperation(NoPutResultSet source,
			GeneratedMethod generationClauses, 
			GeneratedMethod checkGM) throws StandardException{
		super(source.getActivation(),-1,0d,0d);
		this.source = source;
		this.activation = source.getActivation();
		init(SpliceOperationContext.newContext(activation));
	}
	
	public DMLWriteOperation(NoPutResultSet source,
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
		source = (NoPutResultSet)in.readObject();
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
    public RowProvider getMapRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        return ((SpliceOperation)source).getMapRowProvider(top, template);
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        return ((SpliceOperation)source).getReduceRowProvider(top, template);
    }

    @Override
    public void executeShuffle() throws StandardException {
        JobStats jobStats = super.doShuffle();
        JobStatsUtils.logStats(jobStats, LOG);
        Map<String,TaskStats> taskStats = jobStats.getTaskStats();
        long rowsModified = 0;
        for(String taskId:taskStats.keySet()){
            TaskStats stats = taskStats.get(taskId);
            rowsModified+=stats.getWriteStats().getTotalRecords();
        }
        modifiedProvider.rowsModified+=rowsModified;
    }

    @Override
	public void open() throws StandardException {
		SpliceLogUtils.trace(LOG,"Open");
		super.open();
	}

	@Override
	public ExecRow getExecRowDefinition() throws StandardException {
		ExecRow row = ((SpliceOperation)source).getExecRowDefinition();
		SpliceLogUtils.trace(LOG,"execRowDefinition=%s",row);
		return row;
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
        return source.getNextRowCore();
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
		private long rowsModified=0;
		@Override public boolean hasNext() { return false; }

		@Override public ExecRow next() { return null; }

        public void setRowsModified(long rowsModified){
            this.rowsModified = rowsModified;
        }

		@Override
		public void open()  {
			SpliceLogUtils.trace(LOG, "open");
			try {
				source.openCore();
				/* Cache query plan text for source, before it gets blown away */
				if (activation.getLanguageConnectionContext().getRunTimeStatisticsMode())
				{
					/* savedSource nulled after run time statistics generation */
					savedSource = source;
				}
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG, e);
			}
			if(!getNodeTypes().contains(NodeType.REDUCE)){
                try{
                    final Transactor<Put,Get,Scan,Mutation,Result> transactor = TransactorFactoryImpl.getTransactor();
                    final TransactionId childID = transactor.beginChildTransaction(transactor.transactionIdFromString(getTransactionID()), true, true);
                    setChildTransactionID(childID.getTransactionIdString());
                    OperationSink opSink = OperationSink.create(DMLWriteOperation.this,null);
                    TaskStats stats= opSink.sink(getDestinationTable());
                    rowsModified = (int)stats.getReadStats().getTotalRecords();
                    transactor.commit(childID);
                }catch(IOException ioe){
                    if(Exceptions.shouldLogStackTrace(ioe)){
                        SpliceLogUtils.logAndThrowRuntime(LOG,ioe);
                    }else
                        throw new RuntimeException(ioe);
                } finally {
                    clearChildTransactionID();
                }
			}
		}

		@Override
		public void close() {
			SpliceLogUtils.trace(LOG, "close in modifiedProvider for Delete/Insert/Update");
			if (!isOpen)
				return;
			if (isTopResultSet && activation.getLanguageConnectionContext().getRunTimeStatisticsMode() &&
                    !activation.getLanguageConnectionContext().getStatementContext().getStatementWasInvalidated())
				endExecutionTime = getCurrentTimeMillis();
			try {
				source.close();
				isOpen = false;
			} catch (StandardException e) {
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
	
	public NoPutResultSet getSource() {
		return this.source;
	}

    @Override
    public void openCore() throws StandardException {
        super.openCore();
        if(source!=null)source.openCore();
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
}
