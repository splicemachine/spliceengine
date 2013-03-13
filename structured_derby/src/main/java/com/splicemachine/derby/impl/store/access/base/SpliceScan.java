package com.splicemachine.derby.impl.store.access.base;

import com.splicemachine.constants.TxnConstants;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.impl.sql.execute.LazyScan;
import com.splicemachine.derby.impl.sql.execute.ParallelScan;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanInfo;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SpliceScan implements ScanManager, ParallelScan, LazyScan {
	protected static Logger LOG = Logger.getLogger(SpliceScan.class);
	protected OpenSpliceConglomerate spliceConglomerate;
	protected Transaction trans;
	protected byte[] transID;
	protected Scan scan;
	protected FormatableBitSet scanColumnList;
	protected DataValueDescriptor[] startKeyValue;
	protected int startSearchOperator;
	protected Qualifier[][] qualifier;
	protected DataValueDescriptor[] stopKeyValue;
	protected int stopSearchOperator;
	protected ResultScanner scanner;
	protected HTableInterface table;
	protected boolean currentRowDeleted = false;
	protected HBaseRowLocation currentRowLocation;
	protected DataValueDescriptor[] currentRow;
	protected Result currentResult;
	protected long estimatedRowCount = 0;
	protected boolean isKeyed;
	protected boolean scannerInitialized = false;
	protected String tableName;
	public SpliceScan() {
		if (LOG.isTraceEnabled())
			LOG.trace("Instantiate Splice Scan for conglomerate ");
	}

	public SpliceScan(FormatableBitSet scanColumnList,
			DataValueDescriptor[] startKeyValue,
			int startSearchOperator,
			Qualifier[][] qualifier,
			DataValueDescriptor[] stopKeyValue,
			int stopSearchOperator,
			Transaction trans, 
			boolean isKeyed) {
		this.isKeyed = isKeyed;
		this.scanColumnList = scanColumnList;
		this.startKeyValue = startKeyValue;
		this.startSearchOperator = startSearchOperator;
		this.qualifier = qualifier;
		this.stopKeyValue = stopKeyValue;
		this.stopSearchOperator = stopSearchOperator;
		this.trans = trans;
		try {
			((SpliceTransaction)trans).setActiveState();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.transID = SpliceUtils.getTransID(trans);
		if (LOG.isTraceEnabled())
			logIndexKeys();
		setupScan();
		attachFilter();
		tableName = Bytes.toString(SpliceOperationCoprocessor.TEMP_TABLE);
		table = SpliceAccessManager.getHTable(SpliceOperationCoprocessor.TEMP_TABLE);
	}

	
	public SpliceScan(OpenSpliceConglomerate spliceConglomerate,
			FormatableBitSet scanColumnList,
			DataValueDescriptor[] startKeyValue,
			int startSearchOperator,
			Qualifier[][] qualifier,
			DataValueDescriptor[] stopKeyValue,
			int stopSearchOperator,
			Transaction trans, 
			boolean isKeyed) {
		this.spliceConglomerate = spliceConglomerate;
		this.isKeyed = isKeyed;
		this.scanColumnList = scanColumnList;
		this.startKeyValue = startKeyValue;
		this.startSearchOperator = startSearchOperator;
		this.qualifier = qualifier;
		this.stopKeyValue = stopKeyValue;
		this.stopSearchOperator = stopSearchOperator;
		this.trans = trans;
		this.transID = SpliceUtils.getTransID(trans);
		if (LOG.isTraceEnabled())
			logIndexKeys();
		setupScan();
		attachFilter();
		tableName = spliceConglomerate.getConglomerate().getContainerid() + "";
		table = SpliceAccessManager.getHTable(spliceConglomerate.getConglomerate().getContainerid());
	}
	
	public void close() throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("Close");
		try {
			if (scanner != null)
				scanner.close();
			table.close();
		} catch (IOException e) {
			throw StandardException.newException("Error closing scanner and table ",e);
		}
	}

	protected void attachFilter() {
        SpliceLogUtils.trace(LOG,"attaching filter");
		try {
				scan.setFilter(Scans.buildKeyFilter(startKeyValue,2,qualifier));
//            SpliceUtils.attachFilterToScan(scan,qualifier,startKeyValue,2,stopKeyValue,2);
//			FilterList masterList = new FilterList(Operator.MUST_PASS_ALL);
//			if (qualifier != null)
//				masterList.addFilter(SpliceUtils.constructFilter(qualifier));
//			if (startSearchOperator == 1 && stopSearchOperator == 1 && startKeyValue != null && startKeyValue.length == 1
//					&& "".equals(startKeyValue[0].getTraceString().trim())) {
//				LOG.info("NOT GENERATING INDEX FILTER. DO A FULL SCAN.......");
//			} else {
//				if (startKeyValue != null && startSearchOperator >= 0)
//					masterList.addFilter(SpliceUtils.generateIndexFilter(startKeyValue,startSearchOperator));
//				if (stopKeyValue != null && stopSearchOperator >= 0)
//					masterList.addFilter(SpliceUtils.generateIndexFilter(stopKeyValue,stopSearchOperator));
//			}
//		    scan.setFilter(masterList);
		} catch (Exception e) {
			throw new RuntimeException("error attaching Filter",e);
		}
	}
	
	protected void setupScan() {
		if (LOG.isTraceEnabled())
			LOG.trace("setup Scan");
		try {
            boolean[] sortOrder = spliceConglomerate==null?null:
                    ((SpliceConglomerate)this.spliceConglomerate.getConglomerate()).getAscDescInfo();
            scan = Scans.setupScan(startKeyValue,startSearchOperator,stopKeyValue,stopSearchOperator,qualifier,sortOrder,scanColumnList,transID);
//			boolean generateKey = true;
//			if (startKeyValue != null && stopKeyValue != null) {
//				for (int i =0; i<startKeyValue.length; i++) {
//					if (startKeyValue[i].isNull())
//						generateKey = false;
//				}
//			}
//			if (generateKey) {
//				boolean[] sortOrder = null;
//				if (spliceConglomerate != null)
//					sortOrder = ((SpliceConglomerate) this.spliceConglomerate.getConglomerate()).getAscDescInfo();
//				scan.setStartRow(DerbyBytesUtil.generateScanKeyForIndex(startKeyValue,startSearchOperator,sortOrder));
//				scan.setStopRow(DerbyBytesUtil.generateScanKeyForIndex(stopKeyValue,stopSearchOperator,sortOrder));
//				if (scan.getStartRow() != null && scan.getStopRow() != null && Bytes.compareTo(scan.getStartRow(), scan.getStopRow())>=0) {
//					LOG.warn("Scan begin key is greater than the end key");
//				}
//				if (scan.getStartRow() == null)
//					scan.setStartRow(HConstants.EMPTY_START_ROW);
//				if (scan.getStopRow() == null)
//					scan.setStopRow(HConstants.EMPTY_END_ROW);
//			}
		} catch (Exception e) {
			LOG.error("Exception creating start key");
			throw new RuntimeException(e);
		}
	}
	
	public OpenSpliceConglomerate getOpenConglom() {
		return spliceConglomerate;
	}
	
	public boolean delete() throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("delete");
		if (currentResult == null)
			throw StandardException.newException("Attempting to delete with a null current result");
		if (LOG.isTraceEnabled())
			LOG.trace("HBaseScan delete " + currentResult.getRow());
		try {
			Delete delete = new Delete(this.currentResult.getRow());
			if (transID != null)
				delete.setAttribute(TxnConstants.TRANSACTION_ID, transID);
			table.delete(delete);
			currentRowDeleted = true;
			return true;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return false;
		}
	}
		
	public boolean next() throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("next ");
		if (!scannerInitialized)
			initialize();
		currentRowDeleted = false;
		try {
			currentResult = scanner.next();
			if (currentResult != null) 
				this.currentRowLocation = new HBaseRowLocation(currentResult.getRow());
			return currentResult != null;
		} catch (IOException e) {
			throw StandardException.newException("Error calling next() on scan " + e);
		}
	}
	
	public void fetch(DataValueDescriptor[] destRow) throws StandardException {
		if (this.currentResult == null)
			return;
		if (LOG.isTraceEnabled())
			LOG.trace("HBaseScan fetch "+ currentResult.toString());
		fetchWithoutQualify(destRow);
	}
	
	public void didNotQualify() throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("HBaseScan didNotQualify");
	}
	
	public boolean doesCurrentPositionQualify() throws StandardException {
		throw new RuntimeException("Not Implemented");
	}

	public boolean isHeldAfterCommit() throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}
	
	public boolean closeForEndTransaction(boolean closeHeldScan) throws StandardException {
		return false;
	}

	public boolean fetchNext(DataValueDescriptor[] destRow) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("FetchNext " + destRow+" with "+destRow.length);
		
		next();
		if (currentResult != null) {
			fetch(destRow);
			return true;
		} else 
			return false;
	}
	
	public boolean isKeyed() {
		if (LOG.isTraceEnabled())
			LOG.trace("isKeyed");
		return isKeyed;
	}
	
	public boolean isTableLocked() {
		if (LOG.isTraceEnabled())
			LOG.trace("isTableLocked");
		return false;
	}
	
	public ScanInfo getScanInfo() throws StandardException {
		return new SpliceScanInfo(this);
	}
	
	public RowLocation newRowLocationTemplate() throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("newRowLocationTemplate");
		return new HBaseRowLocation();
	}
	
	public boolean isCurrentPositionDeleted() throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("isCurrentPositionDeleted");
		return currentRowDeleted;
	}
	
	public void fetchLocation(RowLocation destRowLocation) throws StandardException {
		if (currentResult == null)
			throw StandardException.newException("currentResult is null ");
		if (LOG.isTraceEnabled())
			LOG.trace("fetchLocation " + currentResult.getRow());
		destRowLocation.setValue(this.currentResult.getRow());		
	}

	public void fetchWithoutQualify(DataValueDescriptor[] destRow) throws StandardException {
		if (LOG.isTraceEnabled()) 
			LOG.trace("HBaseScan fetchWithoutQualify destRow = "+ destRow.toString());

        try{
            if(destRow!=null){
                SpliceUtils.populate(currentResult, scanColumnList, destRow);
		    	this.currentRow = destRow;
            }
            this.currentRowLocation = new HBaseRowLocation(currentResult.getRow());
		} catch (Exception e) {
			throw StandardException.newException("Error occurred during fetch", e);
		}
	}
	/**
	 * This is the right way to do it unfortunately some of the data in OpenSpliceConglomerate are not set
	 * Need to fix it. SO temporarily use the next method to do clone
	 * @return
	 * @throws StandardException
	 */
	protected DataValueDescriptor[] cloneRowTemplate() throws StandardException {
		return spliceConglomerate.cloneRowTemplate();
	}
	
	protected DataValueDescriptor[] cloneRowTemplate(DataValueDescriptor[] original) {
		DataValueDescriptor[] columnClones = new DataValueDescriptor[original.length];
		for (int i = 0; i < original.length; i++) {
			if (original[i] != null) {
                columnClones[i] = original[i].cloneValue(false);
			}
		}
		return columnClones;
	}
	
	public long getEstimatedRowCount() throws StandardException {
		return estimatedRowCount;
	}
	
	public void setEstimatedRowCount(long estimatedRowCount) throws StandardException {
		this.estimatedRowCount = estimatedRowCount;
	}
	public void fetchSet(long max_rowcnt, int[] key_column_numbers,BackingStoreHashtable hashTable) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("IndexScan fetchSet for number of rows " + max_rowcnt);
		if (!scannerInitialized)
			initialize();
		if (max_rowcnt == 0)
			return;
		if (max_rowcnt == -1)
			max_rowcnt = Long.MAX_VALUE;
		int rowCount = 0;
		DataValueDescriptor[] fetchedRow = null;
		try {
			while ((currentResult = scanner.next()) != null) {
				if (LOG.isTraceEnabled())
					LOG.trace("fetch set iterator " + currentResult);
				fetchedRow = RowUtil.newTemplate(
						spliceConglomerate.getTransaction().getDataValueFactory(),
						null, spliceConglomerate.getFormatIds(), spliceConglomerate.getCollationIds());
				SpliceUtils.populate(currentResult, scanColumnList, fetchedRow);
				hashTable.putRow(false, fetchedRow);
				this.currentRowLocation = new HBaseRowLocation(currentResult.getRow());
				rowCount++;
				if (rowCount == max_rowcnt)
					break;
			}				
			this.currentRow = fetchedRow;				
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw StandardException.newException("Error during fetchSet " + e);
		}
	}
	
	public int fetchNextGroup(DataValueDescriptor[][] row_array,RowLocation[] oldrowloc_array, RowLocation[] newrowloc_array) throws StandardException {
		throw new RuntimeException("Not Implemented");
	//	return 0;
	}
	
	public void reopenScan(DataValueDescriptor[] startKeyValue,int startSearchOperator, Qualifier[][] qualifier,DataValueDescriptor[] stopKeyValue, int stopSearchOperator) throws StandardException {
		if (LOG.isTraceEnabled()) 
			LOG.trace("reopenScan startKeyValue " +startKeyValue + ", startSearchOperator " + startSearchOperator + ", qualifier " + qualifier + ", stopKeyValue " + stopKeyValue + ", stopSearchOperator " + stopSearchOperator);
		this.startKeyValue = startKeyValue;
		this.startSearchOperator = startSearchOperator;
		this.qualifier = qualifier;
		this.stopKeyValue = stopKeyValue;
		this.stopSearchOperator = stopSearchOperator;
		setupScan();
		attachFilter();
		try {
			scanner = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	public void reopenScanByRowLocation(RowLocation startRowLocation, Qualifier[][] qualifier) throws StandardException {
		if (LOG.isTraceEnabled()) 
			LOG.trace("reopenScanByRowLocation " +startRowLocation + " for qualifier " + qualifier);

		this.qualifier = qualifier;
		setupScan();
		scan.setStartRow(startRowLocation.getBytes());
		attachFilter();
		try {
			scanner = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	public boolean positionAtRowLocation(RowLocation rl) throws StandardException {
		if (LOG.isTraceEnabled()) 
			LOG.trace("positionAtRowLocation " + rl);
		if (this.currentRowLocation != null)
			return this.currentRowLocation.equals(rl);
		return false;
	}
	
	public int fetchNextGroup(DataValueDescriptor[][] row_array, RowLocation[] rowloc_array) throws StandardException {
		if (LOG.isTraceEnabled()) 
			LOG.trace("HBaseScan fetchNextGroup " +row_array + " for conglomerate " + this.spliceConglomerate.getConglomerate().getId());
		try {
			if (!scannerInitialized)
				initialize();
			if (scanner == null)
				return 0;
			if (row_array == null || row_array.length == 0)
				return 0;
			Result[] results = scanner.next(row_array.length);
			// Have To generate template
			DataValueDescriptor[] template = null;
			if (results != null && results.length > 0) {				
				if (LOG.isTraceEnabled())
					LOG.trace("HBaseScan fetchNextGroup total number of results="+results.length);
				for (int i = 0; i < results.length; i++) {
					if (results[i] != null) {
						if (i == 0)
							template = RowUtil.newRowFromTemplate(row_array[i]);
						row_array[i] = RowUtil.newRowFromTemplate(template);
						SpliceUtils.populate(results[i], scanColumnList, row_array[i]);
					}
				}
				this.currentRowLocation = new HBaseRowLocation(results[results.length-1].getRow());
				this.currentRow = row_array[results.length-1];
				this.currentResult = results[results.length -1];
			}
			return results.length;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw StandardException.newException("Error during fetchNextGroup " + e);
		} 
	}
	
	public boolean replace(DataValueDescriptor[] row,FormatableBitSet validColumns) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("replace values for these valid Columns " + validColumns);
		try {
			table.put(Puts.buildInsert(currentRowLocation.getBytes(), row, validColumns, transID));
			if (validColumns != null)
				table.delete(SpliceUtils.cleanupNullsDelete(new HBaseRowLocation(currentResult.getRow()), row, validColumns, transID)); // Might be faster to cycle through the result
			return true;			
		} catch (Exception e) {
			throw StandardException.newException("Error during replace " + e);
		}
	}
	private void logIndexKeys() {
		try {
			LOG.trace("Instantiate Splice Scan for conglomerate " + spliceConglomerate + ", scanColumnList " + scanColumnList);
			LOG.trace("startSearchOperator " + startSearchOperator);
			if (startKeyValue != null) {
				for (int i =0;i<startKeyValue.length;i++)
					LOG.trace("startkey - "+startKeyValue[i].getTypeName() + " : " + startKeyValue[i].getTraceString());
			}
			LOG.trace("stopSearchOperator " + stopSearchOperator);
			if (stopKeyValue != null) {
				for (int i =0;i<stopKeyValue.length;i++)
					LOG.trace("stopKey - "+stopKeyValue[i].getTypeName() + " : " + stopKeyValue[i].getTraceString());
			}				
		} catch (Exception e) {
			LOG.error("Exception Logging");
		}
	}

	@Override
	public Scan getScan() {
		if (LOG.isTraceEnabled())
			LOG.trace("getScan called from ParallelScan Interface");
		return scan;
	}
	
	public String getTableName() {
		return this.tableName;
	}

	@Override
	public void initialize() {
		if (LOG.isTraceEnabled())
			LOG.trace("initialize on the LazyScan interface");
		try {
			scanner = table.getScanner(scan);
			this.scannerInitialized = true;
		} catch (IOException e) {
			LOG.error("Initializing scanner failed");
			throw new RuntimeException(e);
		}
	}
	
	
}
