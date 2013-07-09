package com.splicemachine.derby.impl.store.access.base;

import com.google.common.io.Closeables;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.impl.sql.execute.LazyScan;
import com.splicemachine.derby.impl.sql.execute.ParallelScan;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.EncodingUtils;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SpliceScan implements ScanManager, ParallelScan, LazyScan {
	protected static Logger LOG = Logger.getLogger(SpliceScan.class);
	protected OpenSpliceConglomerate spliceConglomerate;
	protected Transaction trans;
	protected String transID;
	protected Scan scan;
	protected FormatableBitSet scanColumnList;
    protected RowDecoder rowDecoder;
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
    private int[] rowColumns;
    private MultiFieldDecoder fieldDecoder;

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
			((SpliceTransaction)trans).setActiveState(false, false, false, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.transID = SpliceUtils.getTransID(trans);
		setupScan();
		attachFilter();
		tableName = Bytes.toString(SpliceOperationCoprocessor.TEMP_TABLE);
        this.rowColumns = new int[scanColumnList.size()];
        int pos=0;
        for(int i=scanColumnList.anySetBit();i!=-1;i=scanColumnList.anySetBit(i)){
            rowColumns[pos] = i;
            pos++;
        }
//		table = SpliceAccessManager.getHTable(SpliceOperationCoprocessor.TEMP_TABLE);
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
		setupScan();
		attachFilter();
		tableName = spliceConglomerate.getConglomerate().getContainerid() + "";
        if(scanColumnList!=null){
            this.rowColumns = new int[scanColumnList.size()];
            int pos=0;
            for(int i=scanColumnList.anySetBit();i!=-1;i=scanColumnList.anySetBit(i)){
                rowColumns[pos] = i;
                pos++;
            }
        }
	}
	
	public void close() throws StandardException {
			Closeables.closeQuietly(scanner);
			Closeables.closeQuietly(table);
	}

	protected void attachFilter() {
		try {
            Scans.buildPredicateFilter(startKeyValue, 2, qualifier, null, scan);
		} catch (Exception e) {
			throw new RuntimeException("error attaching Filter",e);
		}
	}
	
	protected void setupScan() {
		try {
            boolean[] sortOrder = spliceConglomerate==null?null:
                    ((SpliceConglomerate)this.spliceConglomerate.getConglomerate()).getAscDescInfo();
            scan = Scans.setupScan(startKeyValue, startSearchOperator, stopKeyValue, stopSearchOperator, qualifier,
                    sortOrder, scanColumnList, transID);
		} catch (Exception e) {
			LOG.error("Exception creating start key");
			throw new RuntimeException(e);
		}
	}
	
	public OpenSpliceConglomerate getOpenConglom() {
		return spliceConglomerate;
	}
	
	public boolean delete() throws StandardException {
		if (currentResult == null)
			throw StandardException.newException("Attempting to delete with a null current result");
		try {
            SpliceUtils.doDelete(table, transID, this.currentResult.getRow());
			currentRowDeleted = true;
			return true;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			return false;
		}
	}
		
	public boolean next() throws StandardException {
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
		fetchWithoutQualify(destRow);
	}
	
	public void didNotQualify() throws StandardException {
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
		SpliceLogUtils.trace(LOG, "fetchLocation %s", currentResult.getRow());
		destRowLocation.setValue(this.currentResult.getRow());		
	}

	public void fetchWithoutQualify(DataValueDescriptor[] destRow) throws StandardException {
        if(fieldDecoder==null)
            fieldDecoder = MultiFieldDecoder.create();
        fieldDecoder.reset();
        try{
            if(destRow!=null){
                for(KeyValue kv:currentResult.raw()){
                    RowMarshaller.sparsePacked().decode(kv, destRow, rowColumns, fieldDecoder);
                }
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
		SpliceLogUtils.trace(LOG, "IndexScan fetchSet for number of rows %d", max_rowcnt);
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
				SpliceLogUtils.trace(LOG,"fetch set iterator %s",currentResult);
                if(fieldDecoder==null)
                    fieldDecoder = MultiFieldDecoder.create();

                fieldDecoder.reset();
				fetchedRow = RowUtil.newTemplate(
						spliceConglomerate.getTransaction().getDataValueFactory(),
						null, spliceConglomerate.getFormatIds(), spliceConglomerate.getCollationIds());
                for(KeyValue kv:currentResult.raw()){
                    RowMarshaller.sparsePacked().decode(kv, fetchedRow, null, fieldDecoder);
                }
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
		this.startKeyValue = startKeyValue;
		this.startSearchOperator = startSearchOperator;
		this.qualifier = qualifier;
		this.stopKeyValue = stopKeyValue;
		this.stopSearchOperator = stopSearchOperator;
		setupScan();
		attachFilter();
        if(table==null)
            table = SpliceAccessManager.getHTable(spliceConglomerate.getConglomerate().getContainerid());
		try {
			scanner = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	public void reopenScanByRowLocation(RowLocation startRowLocation, Qualifier[][] qualifier) throws StandardException {
		SpliceLogUtils.trace(LOG,"reopenScanByRowLocation %s  for qualifier ",startRowLocation,qualifier);
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
		SpliceLogUtils.trace(LOG, "positionAtRowLocation %s", rl);
		if (this.currentRowLocation != null)
			return this.currentRowLocation.equals(rl);
		return false;
	}
	
	public int fetchNextGroup(DataValueDescriptor[][] row_array, RowLocation[] rowloc_array) throws StandardException {
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
				SpliceLogUtils.trace(LOG,"HBaseScan fetchNextGroup total number of results=%d",results.length);
				for (int i = 0; i < results.length; i++) {
                    if(fieldDecoder==null)
                        fieldDecoder = MultiFieldDecoder.create();

                    fieldDecoder.reset();
					if (results[i] != null) {
						if (i == 0)
							template = RowUtil.newRowFromTemplate(row_array[i]);
						row_array[i] = RowUtil.newRowFromTemplate(template);
                        for(KeyValue kv:results[i].raw()){
                            RowMarshaller.sparsePacked().decode(kv, row_array[i], null, fieldDecoder);
                        }
//						SpliceUtils.populate(results[i], scanColumnList, row_array[i]);
					}
				}
				this.currentRowLocation = new HBaseRowLocation(results[results.length-1].getRow());
				this.currentRow = row_array[results.length-1];
				this.currentResult = results[results.length -1];
                return results.length;
			}
            return 0;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw StandardException.newException("Error during fetchNextGroup " + e);
		} 
	}
	
	public boolean replace(DataValueDescriptor[] row, FormatableBitSet validColumns) throws StandardException {
		SpliceLogUtils.trace(LOG, "replace values for these valid Columns %s",validColumns);
		try {
            int[] validCols = SpliceUtils.bitSetToMap(validColumns);
            Put put = SpliceUtils.createPut(currentRowLocation.getBytes(),transID);

            if(entryEncoder==null)
                entryEncoder = EntryEncoder.create(row.length, EncodingUtils.getNonNullColumns(row,validColumns),
                        DerbyBytesUtil.getScalarFields(row),
                        DerbyBytesUtil.getFloatFields(row),
                        DerbyBytesUtil.getDoubleFields(row));

            EncodingUtils.encodeRow(row, put, validCols, validColumns, entryEncoder);

            table.put(put);

//			table.put(Puts.buildInsert(currentRowLocation.getBytes(), row, validColumns, transID));
			return true;
		} catch (Exception e) {
			throw StandardException.newException("Error during replace " + e);
		}
	}
	private void logIndexKeys() {
		try {
			if (startKeyValue != null) {
				for (int i =0;i<startKeyValue.length;i++)
					LOG.trace("startkey - "+startKeyValue[i].getTypeName() + " : " + startKeyValue[i].getTraceString());
			}
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
//		if (LOG.isTraceEnabled())
//			LOG.trace("initialize on the LazyScan interface");
        if(table==null)
            table = SpliceAccessManager.getHTable(spliceConglomerate.getConglomerate().getContainerid());
		try {
			scanner = table.getScanner(scan);
			this.scannerInitialized = true;
		} catch (IOException e) {
			LOG.error("Initializing scanner failed");
			throw new RuntimeException(e);
		}
	}
	
	
}
