package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.stats.ThroughputStats;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
	public void init(SpliceOperationContext context){
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
		
		try {
			reduceScan = Scans.buildPrefixRangeScan(sequence[0], transactionID);
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
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
	public ExecRow getExecRowDefinition() {
//		SpliceLogUtils.trace(LOG, "getExecRowDefinition");
		ExecRow def = ((SpliceOperation)source).getExecRowDefinition();
		source.setCurrentRow(def);
		return def;
	}
	
	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow template){
//		SpliceUtils.setInstructions(reduceScan,getActivation(),top);
		return new ClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,template,null);
	}

	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG,"executeScan");
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
		return new SpliceNoPutResultSet(activation,this,provider);
	}
	
	@Override
	public SinkStats sink() {
		/*
		 * We want to make use of HBase as a sorting mechanism for us.
		 * To that end, we really just want to read all the data
		 * out of source and write it into the TEMP Table.
		 */
        SinkStats.SinkAccumulator stats = SinkStats.uniformAccumulator();
        stats.start();
		SpliceLogUtils.trace(LOG, "sinking with sort based on column %d",orderingItem);
		ExecRow row;
		HTableInterface tempTable = null;
		try{
			Put put;
			tempTable = SpliceAccessManager.getFlushableHTable(SpliceOperationCoprocessor.TEMP_TABLE);
			Hasher hasher = new Hasher(getExecRowDefinition().getRowArray(),keyColumns,descColumns,sequence[0]);
			byte[] tempRowKey;
            Serializer serializer  = new Serializer();

            do{
                long start = System.nanoTime();
                row = getNextRowCore();
                if(row==null)continue;

                stats.processAccumulator().tick(System.nanoTime()-start);

                start = System.nanoTime();
                SpliceLogUtils.trace(LOG, "row="+row);
                if (this.distinct) {
                    tempRowKey = hasher.generateSortedHashKeyWithPostfix(currentRow.getRowArray(),null);
                } else {
                    tempRowKey = hasher.generateSortedHashKeyWithPostfix(currentRow.getRowArray(),SpliceUtils.getUniqueKey());
                }
                put = Puts.buildInsert(tempRowKey,row.getRowArray(),null,serializer);
                tempTable.put(put);

                stats.sinkAccumulator().tick(System.nanoTime()-start);
            }while(row!=null);
			tempTable.flushCommits();
			tempTable.close();
		}catch (StandardException se){
			SpliceLogUtils.logAndThrowRuntime(LOG,se);
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		}finally{
			try {
				if(tempTable!=null)
					tempTable.close();
			} catch (IOException e) {
				SpliceLogUtils.error(LOG, "Unexpected error closing TempTable", e);
			}
		}
        return stats.finish();
	}

	@Override
	public String toString() {
		return "SortOperation {resultSetNumber="+resultSetNumber+",source="+source+"}";
	}

	@Override
	public void openCore() throws StandardException {
		if(source!=null) source.openCore();
	}
	
	
	
}