package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.store.access.hbase.ByteArraySlice;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TableScanOperation extends ScanOperation {
	private static final long serialVersionUID = 3l;

	protected static Logger LOG = Logger.getLogger(TableScanOperation.class);
	protected static List<NodeType> nodeTypes;
	protected int indexColItem;
		public String userSuppliedOptimizerOverrides;
	public int rowsPerRead;
    private List<KeyValue> keyValues;
	protected boolean runTimeStatisticsOn;
	private Properties scanProperties;
	public String startPositionString;
	public String stopPositionString;
	public ByteArraySlice slice;

    private static final MetricName scanTime = new MetricName("com.splicemachine.operations","tableScan","scanTime");
    private final Timer timer = SpliceDriver.driver().getRegistry().newTimer(scanTime, TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
	
	static {
		nodeTypes = Arrays.asList(NodeType.MAP,NodeType.SCAN);
	}

    private EntryDecoder rowDecoder;
    private int[] baseColumnMap;


    public TableScanOperation() {
		super();
	}

    public TableScanOperation(ScanInformation scanInformation,
                              OperationInformation operationInformation,
                              int lockMode,
                              int isolationLevel) throws StandardException {
        super(scanInformation,operationInformation,lockMode,isolationLevel);
    }

    @SuppressWarnings("UnusedParameters")
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
        this.tableName = Long.toString(scanInformation.getConglomerateId());
        this.indexColItem = indexColItem;
        this.indexName = indexName;
        runTimeStatisticsOn = operationInformation.isRuntimeStatisticsEnabled();
        if (LOG.isTraceEnabled())
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
	    keyValues = new ArrayList<KeyValue>(1);
        this.baseColumnMap = operationInformation.getBaseColumnMap();
        this.slice = new ByteArraySlice();
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		return Collections.emptyList();
	}

	@Override
	public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
		SpliceLogUtils.trace(LOG, "getMapRowProvider");
		beginTime = System.currentTimeMillis();
		Scan scan = buildScan(spliceRuntimeContext);
		SpliceUtils.setInstructions(scan, activation, top,spliceRuntimeContext);
		ClientScanProvider provider = new ClientScanProvider("tableScan",Bytes.toBytes(tableName),scan, decoder,spliceRuntimeContext);
		nextTime += System.currentTimeMillis() - beginTime;
		return provider;
	}

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return getMapRowProvider(top, decoder, spliceRuntimeContext);
    }

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				/*
				 * Table Scans only use a key encoder when encoding to SpliceOperationRegionScanner,
				 * in which case, the KeyEncoder should be either the row location of the last emitted
				 * row, or a random field (if no row location is specified).
				 */
				DataHash hash = new SuppliedDataHash(new StandardSupplier<byte[]>() {
						@Override
						public byte[] get() throws StandardException {
								if(currentRowLocation!=null)
										return currentRowLocation.getBytes();
								return SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes();
						}
				});

				return new KeyEncoder(NoOpPrefix.INSTANCE,hash,NoOpPostfix.INSTANCE);
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				ExecRow defnRow = getExecRowDefinition();
				int [] cols = IntArrays.count(defnRow.nColumns());
				return BareKeyHash.encoder(cols,null);
		}

		@Override
	public List<NodeType> getNodeTypes() {
		return nodeTypes;
	}

	@Override
	public ExecRow getExecRowDefinition() {
		return currentTemplate;
	}

    @Override
    public String prettyPrint(int indentLevel) {
        return "Table"+super.prettyPrint(indentLevel);
    }

    @Override
	public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException {
		beginTime = getCurrentTimeMillis();
        long start = System.nanoTime();
        try{
            keyValues.clear();
            regionScanner.next(keyValues);
			if (keyValues.isEmpty()) {
				if (LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG,"%s:no more data retrieved from table",tableName);
				currentRow = null;
				currentRowLocation = null;
			} else {
                if(rowDecoder==null)
                    rowDecoder = new EntryDecoder(SpliceDriver.getKryoPool());
                currentRow.resetRowArray();
                DataValueDescriptor[] fields = currentRow.getRowArray();
                if (fields.length != 0) {
                	for(KeyValue kv:keyValues){
                        //should only be 1
                		RowMarshaller.sparsePacked().decode(kv,fields,baseColumnMap,rowDecoder);
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
                	slice.updateSlice(keyValues.get(0).getBuffer(), keyValues.get(0).getRowOffset(), keyValues.get(0).getRowLength());
                	currentRowLocation.setValue(slice);
//                    currentRowLocation.setValue(keyValues.get(0).getRow()); Removed the Array Copy Here for each row...
                }
			}
		}finally{
            if(SpliceConstants.collectStats)
                timer.update(System.nanoTime()-start,TimeUnit.NANOSECONDS);
        }
		setCurrentRow(currentRow);
		nextTime += getElapsedMillis(beginTime);
		return currentRow;
	}

		@Override
		public String toString() {
				try {
						return String.format("TableScanOperation {tableName=%s,isKeyed=%b,resultSetNumber=%s}",tableName,scanInformation.isKeyed(),resultSetNumber);
				} catch (StandardException e) {
						LOG.error(e); //shouldn't happen
						return String.format("TableScanOperation {tableName=%s,isKeyed=%s,resultSetNumber=%s}", tableName, "UNKNOWN", resultSetNumber);
				}
		}

		@Override
		public void	close() throws StandardException, IOException {
				if(rowDecoder!=null)
						rowDecoder.close();
				SpliceLogUtils.trace(LOG, "close in TableScan");
				beginTime = getCurrentTimeMillis();
//				if ( isOpen ) {
//						clearCurrentRow();

						if (runTimeStatisticsOn)
						{
								// This is where we get the scan properties for a subquery
								scanProperties = getScanProperties();
								startPositionString = printStartPosition();
								stopPositionString = printStopPosition();
						}

						if (forUpdate && scanInformation.isKeyed()) {
								activation.clearIndexScanInfo();
						}

						super.close();

//			if (indexCols != null)
//			{
//				//TODO on index
//			}
//				}
		
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
        try {
            scanProperties.setProperty("columnsFetchedBitSet", ""+scanInformation.getAccessedColumns());
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        }
        //treeHeight
		
		return scanProperties;
	}
}
