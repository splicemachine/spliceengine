package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.hbase.SpliceOperationProtocol;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.storage.RowProviders.SourceRowProvider;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.ZookeeperTransaction;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.*;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.*;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class SpliceBaseOperation implements SpliceOperation, Externalizable, NoPutResultSet {
	private static Logger LOG = Logger.getLogger(SpliceBaseOperation.class);
	protected double optimizerEstimatedRowCount;
	protected double optimizerEstimatedCost;
	protected Activation activation;
	protected int resultSetNumber;
	protected String transactionID;
	protected Transaction trans;
	protected boolean isTopResultSet;
	protected String uniqueSequenceID;
	protected ExecRow currentRow;
	protected RowLocation currentRowLocation;
	protected List<SpliceOperation> leftOperationStack;
	protected List<SpliceOperation> rightOperationStack;

	protected boolean executed = false;
	protected DataValueDescriptor[] sequence;
	protected RegionScanner regionScanner;
	protected long rowsSunk;

	/*
	 * Defines a mapping between any FormattableBitSet's column entries
	 * and a compactRow. This is only used in conjuction with getCompactRow.
	 */
	protected int[] baseColumnMap;

    /*
     * This is actually the parameter set for prepared statements.
     * If this isn't populated correctly, then the serialization framework
     * won't serialize parameters for PreparedStatements over correctly,
     * which will result in failed operations.
     */
    private ParameterValueSet operationParams;

    public SpliceBaseOperation() {
		super();
		if (LOG.isTraceEnabled())
			LOG.trace("instantiated");
	}

	public SpliceBaseOperation(Activation activation, int resultSetNumber, double optimizerEstimatedRowCount,double optimizerEstimatedCost) throws StandardException {
		SpliceLogUtils.trace(LOG,"instantiated for resultSetNumber %d", resultSetNumber);
		this.optimizerEstimatedCost = optimizerEstimatedCost;
		this.optimizerEstimatedRowCount = optimizerEstimatedRowCount;
		this.activation = activation;
		this.resultSetNumber = resultSetNumber;
		this.trans = (activation.getTransactionController() == null) ? null : ((SpliceTransactionManager) activation.getTransactionController()).getRawStoreXact();
		//SpliceLogUtils.trace(LOG,"before seting active, transaction="+trans+",state="+((ZookeeperTransaction)trans).getTransactionStatus()
		//		+",transactionId="+transactionID);
		this.transactionID = (trans == null) ? null : activation.getTransactionController().getActiveStateTxIdString();
		SpliceLogUtils.trace(LOG,"transaction="+trans+",state="+((ZookeeperTransaction)trans).getTransactionStatus()
				+",transactionId="+transactionID);
		this.uniqueSequenceID = SpliceUtils.generateQueryNodeSequence();
		sequence = new DataValueDescriptor[1];
		SpliceLogUtils.trace(LOG, "dataValueFactor=%s",activation.getDataValueFactory());
		sequence[0] = activation.getDataValueFactory().getVarcharDataValue(uniqueSequenceID);

		operationParams = activation.getParameterValueSet().getClone();
	}
	
	public String getTransactioID() {
		return this.transactionID;
	}
	
	public void setTransactionID(String newTransID) {
		this.transactionID = newTransID;
	}
	
	public ExecutionFactory getExecutionFactory(){
		return activation.getExecutionFactory();
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		SpliceLogUtils.trace(LOG, "readExternal");
		optimizerEstimatedCost = in.readDouble();
		optimizerEstimatedRowCount = in.readDouble();
		resultSetNumber = in.readInt();
		transactionID = readNullableString(in);
		isTopResultSet = in.readBoolean();
		uniqueSequenceID = in.readUTF();

        if(in.readBoolean()){
            operationParams = (ParameterValueSet)in.readObject();
        }
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG, "writeExternal");
		out.writeDouble(optimizerEstimatedCost);
		out.writeDouble(optimizerEstimatedRowCount);		
		out.writeInt(resultSetNumber);
		writeNullableString(transactionID, out);
		out.writeBoolean(isTopResultSet);
		out.writeUTF(uniqueSequenceID);


		out.writeBoolean(operationParams!=null);
		if(operationParams!=null){
			out.writeObject(operationParams);
		}
	}

	@Override
	public boolean needsRowLocation() {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public SpliceOperation getLeftOperation() {
		throw new UnsupportedOperationException("class "+this.getClass()+" does not implement getLeftOperation!");
	}

	@Override
	public void rowLocation(RowLocation rl) throws StandardException {
		// TODO Auto-generated method stub
	}
	
	@Override
	public boolean returnsRows() {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public int modifiedRowCount() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public ResultDescription getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Activation getActivation() {
		return activation;
	}
	@Override
	public void open() throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public ExecRow getAbsoluteRow(int row) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow getRelativeRow(int row) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow setBeforeFirstRow() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow getFirstRow() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow getNextRow() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow getPreviousRow() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow getLastRow() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ExecRow setAfterLastRow() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void clearCurrentRow() {
        activation.clearCurrentRow(resultSetNumber);
        currentRow=null;
	}

	@Override
	public boolean checkRowPosition(int isType) throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public int getRowNumber() {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public void close() throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void cleanUp() throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public boolean isClosed() {
		return false;
	}
	@Override
	public void finish() throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public long getExecuteTime() {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public Timestamp getBeginExecutionTimestamp() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Timestamp getEndExecutionTimestamp() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public long getTimeSpent(int type) {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ResultSet getAutoGeneratedKeysResultset() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public String getCursorName() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void addWarning(SQLWarning w) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public SQLWarning getWarnings() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public DataValueDescriptor[] getNextRowFromRowSource()
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public boolean needsToClone() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public FormatableBitSet getValidColumns() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void closeRowSource() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void markAsTopResultSet() {
		this.isTopResultSet = true;		
	}
	@Override
	public void openCore() throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void reopenCore() throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public int getPointOfAttachment() {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public int getScanIsolationLevel() {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public void setTargetResultSet(TargetResultSet trs) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void setNeedsRowLocation(boolean needsRowLocation) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public double getEstimatedRowCount() {
		return this.optimizerEstimatedRowCount;
	}
	@Override
	public int resultSetNumber() {
		return this.resultSetNumber;
	}
	@Override
	public void setCurrentRow(ExecRow row) {
		activation.setCurrentRow(row, resultSetNumber);
		currentRow = row;
	}
	@Override
	public boolean requiresRelocking() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean isForUpdate() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public void updateRow(ExecRow row, RowChanger rowChanger)
			throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void markRowAsDeleted() throws StandardException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void positionScanAtRowLocation(RowLocation rLoc)
			throws StandardException {
		// TODO Auto-generated method stub
		
	}
	
	
	public static String convertScanToString(Scan scan) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		scan.write(dos);
		return Base64.encodeBytes(out.toByteArray());
	}
	 
	public static Scan convertStringToScan(String base64) throws IOException {
		ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(base64));
		DataInputStream dis = new DataInputStream(bis);
		Scan scan = new Scan();
		scan.readFields(dis);
		return scan;
	}
	
	protected ExecRow getCompactRow(LanguageConnectionContext lcc,
																	ExecRow candidate,
																	FormatableBitSet accessedCols,
																	boolean isKeyed) throws StandardException {
		int	numCandidateCols = candidate.nColumns();
		ExecRow compactRow = null;
		if (accessedCols == null) {
			compactRow =  candidate;
			baseColumnMap = new int[numCandidateCols];
			for (int i = 0; i < baseColumnMap.length; i++)
				baseColumnMap[i] = i;
		}
		else {
			int numCols = accessedCols.getNumBitsSet();
			baseColumnMap = new int[numCandidateCols];

			if (compactRow == null) {
				ExecutionFactory ex = lcc.getLanguageConnectionFactory().getExecutionFactory();
				if (isKeyed) {
					compactRow = ex.getIndexableRow(numCols);
				}
				else {
					compactRow = ex.getValueRow(numCols);
				}
			}
			int position = 0;
			for (int i = accessedCols.anySetBit();i != -1; i = accessedCols.anySetBit(i)) {
				// Stop looking if there are columns beyond the columns
				// in the candidate row. This can happen due to the
				// otherCols bit map.
				if (i >= numCandidateCols)
					break;
				DataValueDescriptor sc = candidate.getColumn(i+1);
				if (sc != null) {
					compactRow.setColumn(position + 1,sc);
				}
				baseColumnMap[i] = position;
				position++;
			}
		}

		return compactRow;
	}
	
	public static void writeNullableString(String value, DataOutput out) throws IOException {
		if (value != null) {
			out.writeBoolean(true);
			out.writeUTF(value);			
		} else {
			out.writeBoolean(false);
		}
	}
	
	public static String readNullableString(DataInput in) throws IOException {
		if (in.readBoolean())
			return in.readUTF();
		return null;
	}

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"init called");
		this.activation = context.getActivation();
		//set the parameter value set back on the activation
		if(operationParams!=null){
			try {
				activation.setParameters(operationParams,context.getPreparedStatement().getParameterTypes());
			} catch (StandardException e) {
				SpliceLogUtils.trace(LOG,"Failed to set Parameters on activation",e);
			}
		}
		sequence = new DataValueDescriptor[1];
		sequence[0] = activation.getDataValueFactory().getVarcharDataValue(uniqueSequenceID);
		try {
			this.regionScanner = context.getScanner();
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,"Unable to get Scanner",e);
		}
	}

	@Override
	public String getUniqueSequenceID() {
		return this.uniqueSequenceID;
	}

	@Override		
	public long sink() {
		throw new RuntimeException("Sink Not Implemented for this node " + this.getClass());					
	}

	@Override
	public RowProvider getMapRowProvider(SpliceOperation top,ExecRow template) {
		throw new UnsupportedOperationException("MapRowProviders not implemented for this node: "+ this.getClass());
	}

	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow template) {
		throw new UnsupportedOperationException("ReduceRowProviders not implemented for this node: "+ this.getClass());
	}
	
	@Override
	public void cleanup() {
		throw new RuntimeException("Finish Not Implemented for this node " + this.getClass());														
	}
	
	@Override
	public void executeShuffle() throws StandardException {
		SpliceLogUtils.trace(LOG,"shuffling %s",toString());
		List<SpliceOperation> opStack = getOperationStack();
		SpliceLogUtils.trace(LOG, "operationStack=%s",opStack);
		final SpliceOperation regionOperation = opStack.get(0);
		final SpliceOperation topOperation = opStack.get(opStack.size()-1);
		SpliceLogUtils.trace(LOG,"regionOperation=%s",regionOperation);
		final byte[] table;
		final Scan scan;
		//TODO -sf- deal with situations where we don't have a scan? presumably that's only for local data, 
		//and doesn't need shuffling, but still something to think about
		final RowProvider rowProvider;
		if(regionOperation.getNodeTypes().contains(NodeType.REDUCE) && this != regionOperation){
			rowProvider = regionOperation.getReduceRowProvider(topOperation,topOperation.getExecRowDefinition());
			table = SpliceOperationCoprocessor.TEMP_TABLE;
		}else {
			rowProvider = regionOperation.getMapRowProvider(topOperation,topOperation.getExecRowDefinition());
			table = rowProvider.getTableName();
		}
		scan = rowProvider.toScan();
		if(scan==null||table==null){ 
			if (SourceRowProvider.class.equals(rowProvider.getClass())) {
	    		topOperation.init(SpliceOperationContext.newContext(activation));
	    		topOperation.sink();
	    		return;
	    	} else
	    		throw new AssertionError("Cannot perform shuffle, either scan or table is null: scan="+scan+",table="+table);
		}
		HTableInterface htable = null;
		try{
			htable = SpliceAccessManager.getHTable(table);
			long numberCreated = 0;
            SpliceLogUtils.trace(LOG,"Performing coprocessorExec");

			final Map<byte[], Long> resultMap = new ConcurrentSkipListMap<byte[],Long>(Bytes.BYTES_COMPARATOR);
			final SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),topOperation);
			htable.coprocessorExec(SpliceOperationProtocol.class,scan.getStartRow(),scan.getStopRow(),
													new Batch.Call<SpliceOperationProtocol,Long>(){

				@Override
				public Long call(
						SpliceOperationProtocol instance)
								throws IOException {
					try{
						return instance.run(scan,soi);
					}catch(StandardException se){
						SpliceLogUtils.logAndThrow(LOG, "Unexpected error executing coprocessor",new IOException(se));
						return -1l;
					}
				}
			},new Batch.Callback<Long>(){

						@Override
						public void update(byte[] region, byte[] row, Long result) {
							resultMap.put(region,result);
						}
					});
			for(Long returnedRow : resultMap.values()){
				numberCreated += returnedRow;
			}
			SpliceLogUtils.trace(LOG,"Sunk %d records",numberCreated);
			rowsSunk=numberCreated;
		}catch(IOException ioe){
			if(ioe.getCause() instanceof StandardException)
				SpliceLogUtils.logAndThrow(LOG, (StandardException)ioe.getCause());
			else
				SpliceLogUtils.logAndThrow(LOG,StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION, ioe));
		}catch(Throwable t){
			SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,t));
		}finally{
			if(htable !=null ){
				try{
					htable.close();
				}catch(IOException e){
					SpliceLogUtils.logAndThrow(LOG,"Unable to close Hbase table",
							StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e));
				}
			}
		}
	}

    protected ExecRow getFromResultDescription(ResultDescription resultDescription) throws StandardException {
        ExecRow row = new ValueRow(resultDescription.getColumnCount());
        for(int i=1;i<=resultDescription.getColumnCount();i++){
            ResultColumnDescriptor rcd = resultDescription.getColumnDescriptor(i);
            row.setColumn(i, rcd.getType().getNull());
        }
        return row;
    }

	@Override
	public NoPutResultSet executeScan() throws StandardException {
		throw new RuntimeException("Execute Scan Not Implemented for this node " + this.getClass());														
	}

	@Override
	public NoPutResultSet executeProbeScan() {
		throw new RuntimeException("Execute Probe Scan Not Implemented for this node " + this.getClass());														
	}

	@Override
	public ExecRow getExecRowDefinition() {
		throw new RuntimeException("No ExecRow Definition for this node " + this.getClass());														
	}


	@Override
	public void generateLeftOperationStack(List<SpliceOperation> operations) {
		SpliceLogUtils.trace(LOG, "generateLeftOperationStack");
		OperationUtils.generateLeftOperationStack(this, operations);
	}

	protected List<SpliceOperation> getOperationStack(){
		if(leftOperationStack==null){
			leftOperationStack = new LinkedList<SpliceOperation>();
			generateLeftOperationStack(leftOperationStack);
		}
		return leftOperationStack;
	}
	public void generateRightOperationStack(boolean initial,List<SpliceOperation> operations) {
		SpliceLogUtils.trace(LOG, "generateRightOperationStack");
		SpliceOperation op;
		if (initial) 
			op = getRightOperation();
		else 
			op = getLeftOperation();			
		if(op !=null && !op.getNodeTypes().contains(NodeType.REDUCE)){
			op.generateRightOperationStack(false,operations);
		}else if(op!=null)
			operations.add(op);
		operations.add(this);				
	}

	public List<SpliceOperation> getRightOperationStack(){
		if(rightOperationStack==null){
			rightOperationStack = new LinkedList<SpliceOperation>();
			generateRightOperationStack(true,rightOperationStack);
		}
		return rightOperationStack;
	}
	@Override
	public SpliceOperation getRightOperation() {
		throw new UnsupportedOperationException("class "+this.getClass()+" does not implement getLeftOperation!");
	}
	
	public FormatableBitSet getRootAccessedCols() {
		throw new UnsupportedOperationException("class "+ this.getClass()+" does not implement getRootAccessedCols");
	}
	
}