package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class TableScanOperation extends ScanOperation {
	private static final long serialVersionUID = 3l;

	private static Logger LOG = Logger.getLogger(TableScanOperation.class);
	protected static List<NodeType> nodeTypes;
	protected int indexColItem;
	protected int[] indexCols;
	public String userSuppliedOptimizerOverrides;
	public int rowsPerRead;
    private List<KeyValue> keyValues;
	protected boolean runTimeStatisticsOn;
	private Properties scanProperties;
	public String startPositionString;
	public String stopPositionString;
	protected ThreadLocal<Boolean> initialized = new ThreadLocal<Boolean>() {
		@Override
		protected Boolean initialValue() {
			return false;
		}
		
	};
	
	static {
		nodeTypes = Arrays.asList(NodeType.MAP,NodeType.SCAN);
	}

    private boolean shouldContinue = true;
    private MultiFieldDecoder rowDecoder;


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
        this.forUpdate = forUpdate;
        this.isConstraint = isConstraint;
        this.rowsPerRead = rowsPerRead;
        this.tableName = Long.toString(conglomId);
        this.indexColItem = indexColItem;
        this.indexName = indexName;
        runTimeStatisticsOn = (activation != null && activation.getLanguageConnectionContext().getRunTimeStatisticsMode());
        SpliceLogUtils.trace(LOG, "statisticsTimingOn=%s,isTopResultSet=%s,runTimeStatisticsOn%s",statisticsTimingOn,isTopResultSet,runTimeStatisticsOn);
        init(SpliceOperationContext.newContext(activation));
        recordConstructorTime(); 
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
        super.readExternal(in);
		tableName = in.readUTF();
		indexColItem = in.readInt();
        if(in.readBoolean())
            indexName = in.readUTF();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeUTF(tableName);
		out.writeInt(indexColItem);
        out.writeBoolean(indexName!=null);
        if(indexName!=null)
            out.writeUTF(indexName);
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		super.init(context);
	    keyValues = new ArrayList<KeyValue>(currentRow.nColumns());
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		return Collections.emptyList();
	}

	@Override
	public RowProvider getMapRowProvider(SpliceOperation top,RowDecoder decoder) throws StandardException {
		SpliceLogUtils.trace(LOG, "getMapRowProvider");
		beginTime = System.currentTimeMillis();
		Scan scan = buildScan();
		SpliceUtils.setInstructions(scan, activation, top);
		ClientScanProvider provider = new ClientScanProvider("tableScan",Bytes.toBytes(tableName),scan,decoder);
		nextTime += System.currentTimeMillis() - beginTime;
		return provider;
	}

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, RowDecoder decoder) throws StandardException {
        return getMapRowProvider(top, decoder);
    }

    @Override
    public RowEncoder getRowEncoder() throws StandardException {
        ExecRow row = getExecRowDefinition();
        return RowEncoder.create(row.nColumns(), null, null, null, KeyType.BARE, RowMarshaller.packedCompressed());
    }

    @Override
	public List<NodeType> getNodeTypes() {
//		SpliceLogUtils.trace(LOG,"getNodeTypes");
		return nodeTypes;
	}

	@Override
	public void cleanup() {
		SpliceLogUtils.trace(LOG,"cleanup");
	}

	@Override
	public ExecRow getExecRowDefinition() {
//		SpliceLogUtils.trace(LOG,"getExecRowDefinition");
		return currentTemplate;
	}

    @Override
    public String prettyPrint(int indentLevel) {
        return "Table"+super.prettyPrint(indentLevel);
    }

    @Override
	public ExecRow getNextRowCore() throws StandardException {
		beginTime = getCurrentTimeMillis();
		try {
			  if (!initialized.get()) {
				  region.startRegionOperation();
		      	  MultiVersionConsistencyControl.setThreadReadPoint(regionScanner.getMvccReadPoint());
		      	  initialized.set(true);
			  }			
	        keyValues.clear();
	        regionScanner.nextRaw(keyValues,null);
			if (keyValues.isEmpty()) {
				if (initialized.get()) {
					region.closeRegionOperation();
					initialized.set(false);
				}
				SpliceLogUtils.trace(LOG,"%s:no more data retrieved from table",tableName);
				currentRow = null;
				currentRowLocation = null;
			} else {
                if(rowDecoder==null)
                    rowDecoder = MultiFieldDecoder.create();
                DataValueDescriptor[] fields = currentRow.getRowArray();
                if (fields.length != 0) {
                	for(KeyValue kv:keyValues){
                        //should only be 1
                		RowMarshaller.sparsePacked().decode(kv,fields,null,rowDecoder);
                	}
                }
                if(indexName!=null && currentRow.nColumns() > 0 && currentRow.getColumn(currentRow.nColumns()).getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID){
                    /*
                     * If indexName !=null, then we are currently scanning an index,
                     *so our RowLocation should point to the main table, and not to the
                     * index (that we're actually scanning)
                     */
                    currentRowLocation = (RowLocation) currentRow.getColumn(currentRow.nColumns());
                } else {
                    currentRowLocation.setValue(keyValues.get(0).getRow());
                }
			}
		} catch (Exception e) {
			e.printStackTrace();
			SpliceLogUtils.logAndThrow(LOG, tableName+":Error during getNextRowCore",
																				StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e));
		}
		setCurrentRow(currentRow);
		nextTime += getElapsedMillis(beginTime);
		return currentRow;
	}

	@Override
	public String toString() {
		return String.format("TableScanOperation {tableName=%s,isKeyed=%b,resultSetNumber=%s}",tableName,isKeyed,resultSetNumber);
	}
	
	@Override
	public void	close() throws StandardException
	{
		SpliceLogUtils.trace(LOG, "close in TableScan");
		beginTime = getCurrentTimeMillis();
		if ( isOpen ) {
		    clearCurrentRow();

			if (runTimeStatisticsOn)
				{
					// This is where we get the scan properties for a subquery
					scanProperties = getScanProperties();
					startPositionString = printStartPosition();
					stopPositionString = printStopPosition();
				}
	        	
                if (forUpdate && isKeyed) {
                    activation.clearIndexScanInfo();
                }

			startPosition = null;
			stopPosition = null;

			super.close();

			if (indexCols != null)
			{
				//TODO on index
			}
	    }
		
		closeTime += getElapsedMillis(beginTime);
	}
	
	public Properties getScanProperties()
	{
		//TODO: need to get ScanInfo to store in runtime statistics
		if (scanProperties == null) 
			scanProperties = new Properties();

		scanProperties.setProperty("numPagesVisited", "0");
		scanProperties.setProperty("numRowsVisited", "0");
		scanProperties.setProperty("numRowsQualified", "0"); 
		scanProperties.setProperty("numColumnsFetched", "0");//FIXME: need to loop through accessedCols to figure out
		scanProperties.setProperty("columnsFetchedBitSet", ""+getAccessedCols());
		//treeHeight
		
		return scanProperties;
	}
}
