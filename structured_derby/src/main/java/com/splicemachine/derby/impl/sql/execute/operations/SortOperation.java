
package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

public class SortOperation extends SpliceBaseOperation {
    private static final long serialVersionUID = 2l;
	private static Logger LOG = Logger.getLogger(SortOperation.class);
	private static final List<NodeType> nodeTypes;
	protected NoPutResultSet source;
	protected boolean distinct;
	protected int orderingItem;
	protected int[] keyColumns;
	protected boolean[] descColumns; //descColumns[i] = false => column[i] sorted descending, else sorted ascending
	private ExecRow sortResult;
	private int numColumns;
	private Scan reduceScan;
	private Properties sortProperties = new Properties();
	
	static{
		nodeTypes = Arrays.asList(NodeType.REDUCE,NodeType.SCAN);
	}

    /*
     * Used for serialization. DO NOT USE
     */
    @Deprecated
	public SortOperation(){
//		SpliceLogUtils.trace(LOG, "instantiated without parameters");
	}
	
	public SortOperation(NoPutResultSet s,
						 boolean distinct,
						 int orderingItem,
						 int numColumns,
						 Activation a,
						 GeneratedMethod ra,
						 int resultSetNumber,
						 double optimizerEstimatedRowCount,
						 double optimizerEstimatedCost) throws StandardException{
		super(a,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
//		SpliceLogUtils.trace(LOG,"instantiated with parameters");
//		SpliceLogUtils.trace(LOG,"source="+s);
		this.source = s;
		this.distinct = distinct;
		this.orderingItem = orderingItem;
		this.numColumns = numColumns;
        init(SpliceOperationContext.newContext(a));
        recordConstructorTime(); 
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
//		SpliceLogUtils.trace(LOG, "readExternal");
		super.readExternal(in);
		source = (SpliceOperation)in.readObject();
		distinct = in.readBoolean();
		orderingItem = in.readInt();
		numColumns = in.readInt();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		super.writeExternal(out);
		out.writeObject(source);
		out.writeBoolean(distinct);
		out.writeInt(orderingItem);
		out.writeInt(numColumns);
	}

	@Override
	public List<NodeType> getNodeTypes() {
		return nodeTypes;
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		SpliceLogUtils.trace(LOG,"getSubOperations");
		List<SpliceOperation> ops = new ArrayList<SpliceOperation>();
		ops.add((SpliceOperation)source);
		return ops;
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG,"init");
		super.init(context);
		((SpliceOperation)source).init(context);
		
		FormatableArrayHolder fah = null;
		for(Object o : activation.getPreparedStatement().getSavedObjects()){
			if(o instanceof FormatableArrayHolder){
				fah = (FormatableArrayHolder)o;
				break;
			}
		}
		if(fah==null){
			LOG.error("Unable to find column ordering for sorting!");
			throw new RuntimeException("Unable to find Column ordering for sorting!");
		}
		ColumnOrdering[] order = (ColumnOrdering[])fah.getArray(ColumnOrdering.class);
	
		keyColumns = new int[order.length];
		descColumns = new boolean[order.length];
		descColumns = new boolean[getExecRowDefinition().getRowArray().length];
		
		for(int i =0;i<order.length;i++){
			keyColumns[i] = order[i].getColumnId();
			descColumns[keyColumns[i]] = order[i].getIsAscending();
		}
		
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG,"getNextRowCore");
		sortResult = getNextRowFromSource();
		if(sortResult !=null)
			setCurrentRow(sortResult);
		return sortResult;
	}
	
	private ExecRow getNextRowFromSource() throws StandardException {
		return source.getNextRowCore();
	}

	@Override
	public SpliceOperation getLeftOperation() {
//		SpliceLogUtils.trace(LOG,"getLeftOperation");
		return (SpliceOperation) this.source;
	}
	
	@Override
	public ExecRow getExecRowDefinition() throws StandardException {
//		SpliceLogUtils.trace(LOG, "getExecRowDefinition");
		ExecRow def = ((SpliceOperation)source).getExecRowDefinition();
		source.setCurrentRow(def);
		return def;
	}
	
	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow template) throws StandardException {
        try {
            reduceScan = Scans.buildPrefixRangeScan(sequence[0], getTransactionID());
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
//		SpliceUtils.setInstructions(reduceScan,getActivation(),top);
		return new ClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,template,null);
	}


	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG,"executeScan");
		beginTime = getCurrentTimeMillis();
		final List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
		this.generateLeftOperationStack(opStack);
		SpliceLogUtils.trace(LOG,"operationStack=%s",opStack);
		
		// Get the topmost value, instead of the bottommost, in case it's you
		SpliceOperation regionOperation = opStack.get(opStack.size()-1); 
		SpliceLogUtils.trace(LOG,"regionOperation=%s",regionOperation);
		RowProvider provider;
		if (regionOperation.getNodeTypes().contains(NodeType.REDUCE)){
			provider = regionOperation.getReduceRowProvider(this,getExecRowDefinition());
		}else {
			provider = regionOperation.getMapRowProvider(this,getExecRowDefinition());
		}
		SpliceNoPutResultSet rs =  new SpliceNoPutResultSet(activation,this,provider);
		nextTime += getCurrentTimeMillis() - beginTime;
		return rs;
	}

    @Override
    public OperationSink.Translator getTranslator() throws IOException{
        try{
            final Hasher hasher = new Hasher(getExecRowDefinition().getRowArray(),keyColumns,descColumns,sequence[0]);
            final Serializer serializer = new Serializer();

            return new OperationSink.Translator() {
                @Override
                @Nonnull public List<Mutation> translate(@Nonnull ExecRow row) throws IOException {
                    try {
                        byte[] tempRowKey = distinct?hasher.generateSortedHashKeyWithPostfix(currentRow.getRowArray(),null):
                                hasher.generateSortedHashKeyWithPostfix(currentRow.getRowArray(), SpliceUtils.getUniqueKey());
                        Put put = Puts.buildTempTableInsert(tempRowKey,row.getRowArray(),null,serializer);
                        return Collections.<Mutation>singletonList(put);
                    } catch (StandardException e) {
                        throw Exceptions.getIOException(e);
                    }
                }
            };
        }catch(StandardException se){
            throw Exceptions.getIOException(se);
        }
    }

	@Override
    public RowProvider getMapRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        return ((SpliceOperation)source).getMapRowProvider(top,template);
//        return RowProviders.sourceProvider(top,LOG);
    }
	
	@Override
	public String toString() {
		return "SortOperation {resultSetNumber="+resultSetNumber+",source="+source+"}";
	}

	@Override
	public void openCore() throws StandardException {
        super.openCore();
		if(source!=null) source.openCore();
	}
	
	public NoPutResultSet getSource() {
		return this.source;
	}
	
	public boolean needsDistinct() {
		return this.distinct;
	}
	@Override
	public void	close() throws StandardException
	{ 
		SpliceLogUtils.trace(LOG, "close in Sort");
		beginTime = getCurrentTimeMillis();
		if ( isOpen )
	    {
		    clearCurrentRow();

		    sortResult = null;
			source.close();
			
			super.close();
		}

		closeTime += getElapsedMillis(beginTime);

		isOpen = false;
	}
	
	@Override
	public long getTimeSpent(int type)
	{
		long totTime = constructorTime + openTime + nextTime + closeTime;

		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
		else
			return totTime;
	}
	public Properties getSortProperties() {
		if (sortProperties == null)
			sortProperties = new Properties();
	
		sortProperties.setProperty("numRowsInput", ""+getRowsInput());
		sortProperties.setProperty("numRowsOutput", ""+getRowsOutput());
		return sortProperties;
	}
	
	public long getRowsInput() {
		return getRegionStats() == null ? 0l : getRegionStats().getTotalProcessedRecords();
	}
	
	public long getRowsOutput() {
		return getRegionStats() == null ? 0l : getRegionStats().getTotalSunkRecords();
	}
}
