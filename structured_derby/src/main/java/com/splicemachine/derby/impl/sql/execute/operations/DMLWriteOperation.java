package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

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
	public void init(SpliceOperationContext context){
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
	public void open() throws StandardException {
		SpliceLogUtils.trace(LOG,"Open");
		super.open();
	}

	@Override
	public ExecRow getExecRowDefinition() {
		ExecRow row = ((SpliceOperation)source).getExecRowDefinition();
		SpliceLogUtils.trace(LOG,"execRowDefinition=%s",row);
		return row;
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		return null;
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

	private final RowProvider modifiedProvider = new RowProvider(){
		private long rowsModified=0;
		@Override public boolean hasNext() { return false; }

		@Override public ExecRow next() { return null; }

		@Override public void remove() { throw new UnsupportedOperationException(); }

		@Override
		public void open() {
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
				//For PreparedStatement inserts, on autocommit on, every execute/executeUpdate commits the changes. After 
				//commit, transaction state will be set as IDLE. However since we do not recreate the operations for next 
				//set of values, we stuck with the same idled  transaction. I simply made the transaction active so that 
				//next executeUpdate can be committed. Note setting transaction active is called on caller/client's derby, 
				//not on hbase derby. Thus executeUpdate from caller can trigger another commit. So on and so forth. See
				//Bug 185 - jz
				try {
					if (trans.isIdle()) {
						((SpliceTransaction)trans).setActiveState();
						transactionID = SpliceUtils.getTransIDString(trans);
					}
				} catch (Exception e) {
					SpliceLogUtils.logAndThrowRuntime(LOG, e);
				}
                try{
                    SinkStats stats = sink();
                    rowsModified = (int)stats.getProcessStats().getTotalRecords();
                }catch(IOException ioe){
                    SpliceLogUtils.logAndThrowRuntime(LOG,ioe);
                }
			}
		}

		@Override
		public void close() {
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
	
	public int getModifiedRowCount() {
		if (modifiedProvider != null)
			return modifiedProvider.getModifiedRowCount();
		else
			return (int)rowsSunk;
	}
	
	public NoPutResultSet getSource() {
		return this.source;
	}
}
