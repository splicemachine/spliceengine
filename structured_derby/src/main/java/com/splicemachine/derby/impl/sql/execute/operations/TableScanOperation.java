package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TableScanOperation extends ScanOperation {
	/*
	 * Don't forget to change this every time you make a change that could affect serialization
	 * and/or major class behavior!
	 */
	private static final long serialVersionUID = 2l;

	private static Logger LOG = Logger.getLogger(TableScanOperation.class);
	protected String mapTableName;
	protected static List<NodeType> nodeTypes;
	protected int indexColItem;
	protected int[] indexCols;
	protected String indexName;
	protected Result result;
	static {
		nodeTypes = Arrays.asList(NodeType.MAP,NodeType.SCAN);
	}

	public TableScanOperation() {
		super();
	}

    public  TableScanOperation(long conglomId,
                               StaticCompiledOpenConglomInfo scoci,
                               Activation activation,
                               GeneratedMethod resultRowAllocator,
                               int resultSetNumber,
                               GeneratedMethod startKeyGetter, int startSearchOperator,
                               GeneratedMethod stopKeyGetter, int stopSearchOperator,
                               boolean sameStartStopPosition,
                               String qualifiersField,
                               String tableName,
                               String userSuppliedOptimizerOverrides,
                               String indexName,
                               boolean isConstraint,
                               boolean forUpdate,
                               int colRefItem,
                               int indexColItem,
                               int lockMode,
                               boolean tableLocked,
                               int isolationLevel,
                               int rowsPerRead,
                               boolean oneRowScan,
                               double optimizerEstimatedRowCount,
                               double optimizerEstimatedCost) throws StandardException {
        super(conglomId,activation,resultSetNumber,startKeyGetter,startSearchOperator,stopKeyGetter,stopSearchOperator,
                sameStartStopPosition,qualifiersField, resultRowAllocator,lockMode,tableLocked,isolationLevel,
                colRefItem,optimizerEstimatedRowCount,optimizerEstimatedCost);
        SpliceLogUtils.trace(LOG,"instantiated for tablename %s or indexName %s with conglomerateID %d",
                tableName,indexName,conglomId);
        this.mapTableName = Long.toString(conglomId);
        this.indexColItem = indexColItem;
        this.indexName = indexName;
        init(SpliceOperationContext.newContext(activation));
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
        SpliceLogUtils.trace(LOG,"readExternal");
        super.readExternal(in);
		mapTableName = in.readUTF();
		indexColItem = in.readInt();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG,"writeExternal");
		super.writeExternal(out);
		out.writeUTF(mapTableName);
		out.writeInt(indexColItem);
	}

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"init called for tableName %s",mapTableName);
		super.init(context);
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		return Collections.emptyList();
	}

	@Override
	public RowProvider getMapRowProvider(SpliceOperation top,ExecRow template){
		SpliceLogUtils.trace(LOG, "getMapRowProvider");
		Scan scan = buildScan();
		SpliceUtils.setInstructions(scan, activation, top);
		return new ClientScanProvider(Bytes.toBytes(mapTableName),scan,template,null);
	}

	@Override
	public List<NodeType> getNodeTypes() {
		SpliceLogUtils.trace(LOG,"getNodeTypes");
		return nodeTypes;
	}

	@Override
	public void cleanup() {
		SpliceLogUtils.trace(LOG,"cleanup");
	}

	@Override
	public ExecRow getExecRowDefinition() {
		SpliceLogUtils.trace(LOG,"getExecRowDefinition");
		return currentTemplate;
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG,"%s:getNextRowCore",mapTableName);
		List<KeyValue> keyValues = new ArrayList<KeyValue>();
		try {
			regionScanner.next(keyValues);
			if (keyValues.isEmpty()) {
				SpliceLogUtils.trace(LOG,"%s:no more data retrieved from table",mapTableName);
				currentRow = null;
				currentRowLocation = null;
			} else {
				result = new Result(keyValues);
				SpliceUtils.populate(result, currentRow.getRowArray(), accessedCols,baseColumnMap);
				currentRowLocation = new HBaseRowLocation(result.getRow());
			}
		} catch (Exception e) {
			SpliceLogUtils.logAndThrow(LOG, mapTableName+":Error during getNextRowCore",
																				StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e));
		}
		setCurrentRow(currentRow);
		SpliceLogUtils.trace(LOG,"<%s> emitting %s",mapTableName,currentRow);
		return currentRow;
	}

	@Override
	public String toString() {
		return String.format("TableScanOperation {mapTableName=%s,isKeyed=%b,resultSetNumber=%s}",mapTableName,isKeyed,resultSetNumber);
	}

    @Override
    public void close() throws StandardException {
        SpliceLogUtils.trace(LOG, "closing");
        super.close();
    }
}
