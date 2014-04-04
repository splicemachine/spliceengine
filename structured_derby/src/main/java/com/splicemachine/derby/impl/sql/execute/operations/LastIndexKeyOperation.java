package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
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
    private RegionScanner tentativeScanner;

		private SITableScanner tableScanner;

    static {
        nodeTypes = Arrays.asList(NodeType.MAP, NodeType.SCAN);
    }

		public LastIndexKeyOperation() {
				super();
		}

		public LastIndexKeyOperation
						(
										Activation 			activation,
										int 				resultSetNumber,
										GeneratedMethod 	resultRowAllocator,
										long 				conglomId,
										String 				tableName,
										String 				userSuppliedOptimizerOverrides,
										String 				indexName,
										int 				colRefItem,
										int 				lockMode,
										boolean				tableLocked,
										int					isolationLevel,
										double				optimizerEstimatedRowCount,
										double 				optimizerEstimatedCost
						) throws StandardException {
				super(conglomId, activation, resultSetNumber, null, -1, null, -1,
								true, null, resultRowAllocator, lockMode, tableLocked, isolationLevel,
								colRefItem, optimizerEstimatedRowCount, optimizerEstimatedCost);
				this.tableName = Long.toString(scanInformation.getConglomerateId());
				this.indexName = indexName;
				init(SpliceOperationContext.newContext(activation));
				returnedRow = false;
				recordConstructorTime();
		}


    private static final byte [] LAST_ROW = new byte [128];
    static {
        Arrays.fill(LAST_ROW, (byte) 0xff);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
				this.baseColumnMap = operationInformation.getBaseColumnMap();
        startExecutionTime = System.currentTimeMillis();
        contextScan = context.getScan();
		}

    private BufferedRegionScanner getTentativeScanner(Scan contextScan) throws IOException {
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
        return new BufferedRegionScanner(region,baseScanner,scan, SpliceConstants.DEFAULT_CACHE_SIZE, new SpliceRuntimeContext());
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
						tentativeScanner = getTentativeScanner(contextScan);
						if(tableScanner==null){
								RegionScanner scanner = tentativeScanner!=null? tentativeScanner: regionScanner;
								tableScanner = new SITableScanner(scanner,currentRow,
												spliceRuntimeContext,contextScan,baseColumnMap,
												transactionID,scanInformation.getAccessedPkColumns(),indexName);
						}

						// First we try to get the last row starting close to the end of the region
//						if (tentativeScanner != null) {
						currentRow = null;
						ExecRow lastRow;
						boolean isTentative = tentativeScanner!=null;
						do{
								lastRow = tableScanner.next(spliceRuntimeContext);
								if(lastRow==null && isTentative){
										tableScanner.setRegionScanner(regionScanner);
										lastRow = tableScanner.next(spliceRuntimeContext);
								}
								if(lastRow!=null)
										currentRow = lastRow;
						}while(lastRow!=null);
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

            ExecRow currentRow = null;
            // First we try to get the last row starting close to the end of the region
            tentativeScanner = getTentativeScanner(contextScan);
            if (tentativeScanner != null) {
                currentRow = nextFromRegionScanner(tentativeScanner);
            }
            if (currentRow == null) {
                // If we didn't find any suitable row starting from the end, scan the whole region
                currentRow = nextFromRegionScanner(regionScanner);
            }
            if (currentRow != null) {
                if (indexName != null && currentRow.nColumns() > 0 && currentRow.getColumn(currentRow.nColumns()).getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID) {
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

            returnedRow = true;
            setCurrentRow(currentRow);
            timer.stopTiming();

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

    private ExecRow nextFromRegionScanner(RegionScanner regionScanner) throws IOException, StandardException {
        KeyValue[] prev = new KeyValue[2];
        keyValues.clear();
        regionScanner.next(keyValues);
        ExecRow currentRow = getExecRowDefinition();

        if (keyValues.isEmpty()) {
            return null;
        }
        while (!keyValues.isEmpty()) {
            keyValues.toArray(prev);
            keyValues.clear();
            rowsFiltered++;
            regionScanner.next(keyValues);
        }

        Collections.addAll(keyValues, prev);
        currentRow.resetRowArray();
        DataValueDescriptor[] fields = currentRow.getRowArray();

        for (KeyValue kv : keyValues) {
            //should only be 1
            if (kv != null) {
                RowMarshaller.sparsePacked().decode(kv, fields, baseColumnMap, getRowDecoder());
                if (scanInformation.getAccessedPkColumns() != null &&
                        scanInformation.getAccessedPkColumns().getNumBitsSet() > 0) {
                    getKeyMarshaller().decode(kv, fields, baseColumnMap, getKeyDecoder(),
                            getColumnOrdering(), getColumnDVDs());
                }
            }
        }

        return currentRow;
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
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
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException {
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
        stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS, regionScanner.getRowsOutput());
        stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES, regionScanner.getBytesOutput());
        TimeView localScanTime = regionScanner.getReadTime();
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
