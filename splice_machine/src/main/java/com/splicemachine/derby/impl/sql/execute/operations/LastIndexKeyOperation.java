package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.utils.SpliceLogUtils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LastIndexKeyOperation extends ScanOperation {

    private static Logger LOG = Logger.getLogger(LastIndexKeyOperation.class);
		private int[] baseColumnMap;
    protected static List<NodeType> nodeTypes;
    private boolean returnedRow;
    private Scan contextScan;
    private MeasuredRegionScanner tentativeScanner;

		private SITableScanner tableScanner;

	    protected static final String NAME = LastIndexKeyOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}

		
    static {
        nodeTypes = Arrays.asList(NodeType.MAP, NodeType.SCAN);
    }


		public LastIndexKeyOperation() {
        super();
    }

    public LastIndexKeyOperation
            (
                    Activation activation,
                    int resultSetNumber,
                    GeneratedMethod resultRowAllocator,
                    long conglomId,
                    String tableName,
                    String userSuppliedOptimizerOverrides,
                    String indexName,
                    int colRefItem,
                    int lockMode,
                    boolean tableLocked,
                    int isolationLevel,
                    double optimizerEstimatedRowCount,
                    double optimizerEstimatedCost
            ) throws StandardException {
        super(conglomId, activation, resultSetNumber, null, -1, null, -1,
                true, false, null, resultRowAllocator, lockMode, tableLocked, isolationLevel,
                colRefItem, -1, false,optimizerEstimatedRowCount, optimizerEstimatedCost,userSuppliedOptimizerOverrides);
        this.tableName = Long.toString(scanInformation.getConglomerateId());
        this.indexName = indexName;
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				returnedRow = false;
        recordConstructorTime();
    }


    private static final byte [] LAST_ROW = new byte [128];
    static {
        Arrays.fill(LAST_ROW, (byte) 0xff);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
				this.baseColumnMap = operationInformation.getBaseColumnMap();
        startExecutionTime = System.currentTimeMillis();
        contextScan = context.getScan();
    }

    private BufferedRegionScanner getTentativeScanner(Scan contextScan, SpliceRuntimeContext spliceRuntimeContext) throws IOException {
        if(region==null)return null;
        byte[] endKey = region.getRegionInfo().getEndKey();
        if (endKey.length == 0) {
            // last region, use LAST_ROW
            endKey = LAST_ROW;
        } else if (endKey[endKey.length - 1] == 0) {
            byte[] copy = new byte[endKey.length - 1];
            System.arraycopy(endKey, 0, copy, 0, endKey.length - 1);
						endKey = copy;
        } else {
            byte[] copy = new byte[endKey.length];
            System.arraycopy(endKey, 0, copy, 0, endKey.length - 1);
            int last = endKey[endKey.length - 1] & 0xFF;
            last--;
            copy[endKey.length - 1] = (byte) last;
            endKey = copy;
        }
        Result closestRowBefore = null;
        try {
            closestRowBefore = region.getClosestRowBefore(endKey, SpliceConstants.DEFAULT_FAMILY_BYTES);
        } catch (Exception e) {
            // getClosestRowBefore is not super stable, it might fail sometimes, just bail out
            LOG.warn("Error while trying getClosestRowBefore optimization", e);
        }
        if (closestRowBefore == null) {
            return null;
        }
        byte [] scanStartKey = closestRowBefore.getRow();
        Scan scan = new Scan(contextScan);
        scan.setCacheBlocks(true);
        scan.setStartRow(scanStartKey);
        RegionScanner baseScanner = region.getCoprocessorHost().preScannerOpen(scan);
        if (baseScanner == null) {
            baseScanner = region.getScanner(scan);
        }
        return new BufferedRegionScanner(region,baseScanner,scan, SpliceConstants.DEFAULT_CACHE_SIZE, spliceRuntimeContext,HTransactorFactory.getTransactor().getDataLib());
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.emptyList();
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if (returnedRow) {
						currentRow = null;
						currentRowLocation = null;
						stopExecutionTime = System.currentTimeMillis();
				}else{

						ExecRow currentRow = getExecRowDefinition();
						tentativeScanner = getTentativeScanner(contextScan, spliceRuntimeContext);
						if(tableScanner==null){
								MeasuredRegionScanner scanner = tentativeScanner!=null? tentativeScanner: regionScanner;
								tableScanner = new TableScannerBuilder()
												.scanner(scanner)
                        .region(txnRegion)
												.scan(contextScan)
                        .transaction(operationInformation.getTransaction())
												.template(currentRow)
												.metricFactory(spliceRuntimeContext)
												.rowDecodingMap(baseColumnMap)
												.keyColumnEncodingOrder(scanInformation.getColumnOrdering())
												.keyColumnSortOrder(scanInformation.getConglomerate().getAscDescInfo())
												.keyColumnTypes(getKeyFormatIds())
												.keyDecodingMap(getKeyDecodingMap())
												.accessedKeyColumns(scanInformation.getAccessedPkColumns())
												.indexName(indexName)
												.tableVersion(scanInformation.getTableVersion())
												.build();
						}

						// First we try to get the last row starting close to the end of the region
//						if (tentativeScanner != null) {
						currentRow = null;
						ExecRow lastRow;
						boolean isTentative = tentativeScanner!=null;
						boolean shouldContinue;
						do{
								lastRow = tableScanner.next(spliceRuntimeContext);
								shouldContinue = lastRow!=null;
								if(lastRow==null){
										if(currentRow!=null){
												shouldContinue=false;
										}else if(isTentative){
												tableScanner.setRegionScanner(regionScanner);
												isTentative=false;
												shouldContinue=true;
										}
								}else if(currentRow==null){
										currentRow = lastRow.getClone();
										currentRowLocation = (RowLocation)tableScanner.getCurrentRowLocation().cloneValue(true);
								}else{
										DataValueDescriptor[] currentFields = currentRow.getRowArray();
										DataValueDescriptor[] lastFields = lastRow.getRowArray();
										for(int i=0;i<lastFields.length;i++){
												currentFields[i].setValue(lastFields[i]);
										}
										currentRowLocation.setValue(tableScanner.getCurrentRowLocation());
								}
						}while(shouldContinue);
//						currentRow = tableScanner.next(spliceRuntimeContext);
////								currentRow = nextFromRegionScanner(tentativeScanner);
////						}
//						if (currentRow == null) {
//								// If we didn't find any suitable row starting from the end, scan the whole region
//								currentRow = tableScanner.next(spliceRuntimeContext);
////								currentRow = nextFromRegionScanner(regionScanner);
//						}
//						if (currentRow != null) {
//								if (indexName != null && currentRow.nColumns() > 0 && currentRow.getColumn(currentRow.nColumns()).getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID) {
//                    /*
//                     * If indexName !=null, then we are currently scanning an index,
//                     *so our RowLocation should point to the main table, and not to the
//                     * index (that we're actually scanning)
//                     */
//										currentRowLocation = (RowLocation) currentRow.getColumn(currentRow.nColumns());
//								} else {
//										currentRowLocation.setValue(keyValues.get(0).getRow());
//								}
//						}

            returnedRow = true;
            setCurrentRow(currentRow);
						currentRowLocation = tableScanner.getCurrentRowLocation();

            stopExecutionTime = System.currentTimeMillis();
        }
        return currentRow;
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();

        if(tentativeScanner!=null)
            tentativeScanner.close();
    }

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        SpliceLogUtils.trace(LOG, "getMapRowProvider");
        beginTime = System.currentTimeMillis();

        Scan scan = getNonSIScan(spliceRuntimeContext);

        SpliceUtils.setInstructions(scan, activation, top, spliceRuntimeContext);
        ClientScanProvider provider = new ClientScanProvider("LastIndexKey", Bytes.toBytes(tableName), scan,
                OperationUtils.getPairDecoder(this, spliceRuntimeContext), spliceRuntimeContext);
        nextTime += System.currentTimeMillis() - beginTime;
        return provider;
    }

    @Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				return getMapRowProvider(top, decoder, spliceRuntimeContext);
		}

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        tableName = in.readUTF();
        if (in.readBoolean())
            indexName = in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(tableName);
        out.writeBoolean(indexName != null);
        if (indexName != null)
            out.writeUTF(indexName);
    }

    @Override
    protected int getNumMetrics() {
        return super.getNumMetrics() + 5;
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats) {

        MeasuredRegionScanner scanner = tentativeScanner!=null? tentativeScanner: regionScanner;
        stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS, scanner.getRowsOutput());
        stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES, scanner.getBytesOutput());
        TimeView localScanTime = scanner.getReadTime();
        stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME, localScanTime.getWallClockTime());
        stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME, localScanTime.getCpuTime());
        stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME, localScanTime.getUserTime());
        stats.addMetric(OperationMetric.FILTERED_ROWS, rowsFiltered);
    }

    @Override
    public ExecRow getExecRowDefinition() {
        return currentTemplate;
    }

}
