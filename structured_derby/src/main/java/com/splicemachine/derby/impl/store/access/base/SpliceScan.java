package com.splicemachine.derby.impl.store.access.base;

import java.io.IOException;

import com.google.common.io.Closeables;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanInfo;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.LazyScan;
import com.splicemachine.derby.impl.sql.execute.ParallelScan;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceScan implements ScanManager, ParallelScan, LazyScan {
    protected static Logger LOG = Logger.getLogger(SpliceScan.class);
    protected OpenSpliceConglomerate spliceConglomerate;
    protected Transaction trans;
    protected String transID;
    protected Scan scan;
    protected FormatableBitSet scanColumnList;
    //    protected RowDecoder rowDecoder;
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
    //		private EntryEncoder entryEncoder;
    private EntryDataHash entryEncoder;
    private EntryDecoder entryDecoder;

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
            ((SpliceTransaction) trans).setActiveState(false, false, false, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.transID = SpliceUtils.getTransID(trans);
        setupScan();
        attachFilter();
        tableName = SpliceConstants.TEMP_TABLE;
        setupRowColumns();
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
        setupRowColumns();
    }

    /**
     * Initialize the row columns for this scanner.  The index of the array is the position of the field in the
     * HBase "record".
     * The value of the array element is the position in the returned row (ExecRow).  If the field is not present
     * in the returned row,
     * -1 is set as the value.
     */
    private void setupRowColumns() {
        if (scanColumnList != null) {
            int size = scanColumnList.size();
            rowColumns = new int[size];
            for (int i = 0; i < size; i++) {
                rowColumns[i] = (scanColumnList.isSet(i) ? i : -1);
            }
        }
    }

    public void close() throws StandardException {
        if (entryDecoder != null)
            entryDecoder.close();

        Closeables.closeQuietly(scanner);
        Closeables.closeQuietly(table);
    }

    public ScanInfo getScanInfo() throws StandardException {
        return new SpliceScanInfo(this);
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

    public RowLocation newRowLocationTemplate() throws StandardException {
        if (LOG.isTraceEnabled())
            LOG.trace("newRowLocationTemplate");
        return new HBaseRowLocation();
    }

    public void reopenScan(DataValueDescriptor[] startKeyValue, int startSearchOperator, Qualifier[][] qualifier,
                           DataValueDescriptor[] stopKeyValue, int stopSearchOperator) throws StandardException {
        this.startKeyValue = startKeyValue;
        this.startSearchOperator = startSearchOperator;
        this.qualifier = qualifier;
        this.stopKeyValue = stopKeyValue;
        this.stopSearchOperator = stopSearchOperator;
        setupScan();
        attachFilter();
        if (table == null)
            table = SpliceAccessManager.getHTable(spliceConglomerate.getConglomerate().getContainerid());
        try {
            scanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void reopenScanByRowLocation(RowLocation startRowLocation, Qualifier[][] qualifier) throws
        StandardException {
        SpliceLogUtils.trace(LOG, "reopenScanByRowLocation %s  for qualifier ", startRowLocation, qualifier);
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

    protected void attachFilter() {
        try {
            Scans.buildPredicateFilter(
                qualifier,
                null,
                spliceConglomerate.getColumnOrdering(),
                spliceConglomerate.getFormatIds(),
                scan, "1.0");
        } catch (Exception e) {
            throw new RuntimeException("error attaching Filter", e);
        }
    }

    protected void setupScan() {
        try {
            boolean[] sortOrder = spliceConglomerate == null ? null :
                ((SpliceConglomerate) this.spliceConglomerate.getConglomerate()).getAscDescInfo();
            boolean sameStartStop = isSameStartStop(startKeyValue, startSearchOperator, stopKeyValue,
                                                    stopSearchOperator);
            scan = Scans.setupScan(startKeyValue, startSearchOperator, stopKeyValue, stopSearchOperator, qualifier,
                                   sortOrder, scanColumnList, transID, sameStartStop,
                                   ((SpliceConglomerate) this.spliceConglomerate.getConglomerate()).format_ids,
                                   ((SpliceConglomerate) this.spliceConglomerate.getConglomerate()).columnOrdering,
                                   ((SpliceConglomerate) this.spliceConglomerate.getConglomerate()).columnOrdering,
                                   null, "1.0");
        } catch (Exception e) {
            LOG.error("Exception creating start key");
            throw new RuntimeException(e);
        }
    }

    private boolean isSameStartStop(DataValueDescriptor[] startKeyValue, int startSearchOperator,
                                    DataValueDescriptor[] stopKeyValue, int stopSearchOperator) throws
        StandardException {
                /*
				 * Determine if the start and stop operators are actually, in fact the same.
				 *
				 * This assumes that the start and stop key operators are actually of the same type. While
				 * I don't think that this is a bad assumption, I suppose it could be in some circumstances.
				 */
        if (startSearchOperator != stopSearchOperator) return false;

        if (startKeyValue == null) {
            return stopKeyValue == null;
        } else if (stopKeyValue == null) return false;
        for (int i = 0; i < startKeyValue.length; i++) {
            if (i >= stopKeyValue.length) return false;
            DataValueDescriptor startDvd = startKeyValue[i];
            DataValueDescriptor stopDvd = stopKeyValue[i];
            if (startDvd.getTypeFormatId() != stopDvd.getTypeFormatId()) return false;
            if (startDvd.compare(stopDvd) != 0) return false;
        }
        return true;
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

    public void didNotQualify() throws StandardException {
    }

    public boolean doesCurrentPositionQualify() throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    public boolean isHeldAfterCommit() throws StandardException {
        // TODO Auto-generated method stub
        return false;
    }

    public void fetch(DataValueDescriptor[] destRow) throws StandardException {
        if (this.currentResult == null)
            return;
        fetchWithoutQualify(destRow);
    }

    public void fetchWithoutQualify(DataValueDescriptor[] destRow) throws StandardException {
//				if(entryDecoder==null)
//						entryDecoder = new EntryDecoder(SpliceDriver.getKryoPool());
        try {
            if (destRow != null) {
                ExecRow row = new ValueRow(destRow.length);
                row.setRowArray(destRow);
                DescriptorSerializer[] serializers = VersionedSerializers.forVersion("1.0",
                                                                                     true).getSerializers(destRow);
                EntryDataDecoder decoder = new EntryDataDecoder(null, null, serializers);
                try {
                    Cell kv = CellUtils.matchDataColumn(currentResult.rawCells());
                    decoder.set(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
                    decoder.decode(row);
                    this.currentRow = destRow;
                } finally {
                    Closeables.closeQuietly(decoder);
                }
            }
            this.currentRowLocation = new HBaseRowLocation(currentResult.getRow());
        } catch (Exception e) {
            throw StandardException.newException("Error occurred during fetch", e);
        }
    }

    public boolean fetchNext(DataValueDescriptor[] destRow) throws StandardException {
        next();
        if (currentResult != null) {
            fetch(destRow);
            return true;
        } else
            return false;
    }

    public void fetchLocation(RowLocation destRowLocation) throws StandardException {
        if (currentResult == null)
            throw StandardException.newException("currentResult is null ");
        SpliceLogUtils.trace(LOG, "fetchLocation %s", currentResult.getRow());
        destRowLocation.setValue(this.currentResult.getRow());
    }

    public boolean isCurrentPositionDeleted() throws StandardException {
        if (LOG.isTraceEnabled())
            LOG.trace("isCurrentPositionDeleted");
        return currentRowDeleted;
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

    public boolean positionAtRowLocation(RowLocation rl) throws StandardException {
        SpliceLogUtils.trace(LOG, "positionAtRowLocation %s", rl);
        if (this.currentRowLocation != null)
            return this.currentRowLocation.equals(rl);
        return false;
    }

    public boolean replace(DataValueDescriptor[] row, FormatableBitSet validColumns) throws StandardException {
        SpliceLogUtils.trace(LOG, "replace values for these valid Columns %s", validColumns);
        try {
            int[] validCols = SpliceUtils.bitSetToMap(validColumns);
            Put put = SpliceUtils.createPut(currentRowLocation.getBytes(), transID);

            DescriptorSerializer[] serializers = VersionedSerializers.forVersion("1.0", true).getSerializers(row);
            entryEncoder = new EntryDataHash(validCols, null, serializers);
            ExecRow execRow = new ValueRow(row.length);
            execRow.setRowArray(row);
            entryEncoder.setRow(execRow);
            byte[] data = entryEncoder.encode();
            put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES, data);

            table.put(put);

//			table.put(Puts.buildInsert(currentRowLocation.getByteCopy(), row, validColumns, transID));
            return true;
        } catch (Exception e) {
            throw StandardException.newException("Error during replace " + e);
        }
    }

    public boolean closeForEndTransaction(boolean closeHeldScan) throws StandardException {
        return false;
    }

    public void fetchSet(long max_rowcnt, int[] key_column_numbers, BackingStoreHashtable hashTable) throws
        StandardException {
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
                SpliceLogUtils.trace(LOG, "fetch set iterator %s", currentResult);
                if (entryDecoder == null)
                    entryDecoder = new EntryDecoder();

                fetchedRow = RowUtil.newTemplate(
                    spliceConglomerate.getTransaction().getDataValueFactory(),
                    null, spliceConglomerate.getFormatIds(), spliceConglomerate.getCollationIds());
                DescriptorSerializer[] serializers = VersionedSerializers.forVersion("1.0",
                                                                                     true).getSerializers(fetchedRow);
                EntryDataDecoder decoder = new EntryDataDecoder(null, null, serializers);
                try {
                    ExecRow row = new ValueRow(fetchedRow.length);
                    row.setRowArray(fetchedRow);
                    Cell kv = CellUtils.matchDataColumn(currentResult.raw());
                    decoder.set(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
                    decoder.decode(row);
                } finally {
                    Closeables.closeQuietly(decoder);
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

    /**
     * This is the right way to do it unfortunately some of the data in OpenSpliceConglomerate are not set
     * Need to fix it. SO temporarily use the next method to do clone
     *
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
            if (results != null && results.length > 0) {
                SpliceLogUtils.trace(LOG, "HBaseScan fetchNextGroup total number of results=%d", results.length);
                for (int i = 0; i < results.length; i++) {
                    DataValueDescriptor[] kdvds = row_array[i];
                    ExecRow row = new ValueRow(kdvds.length);
                    row.setRowArray(kdvds);
                    DescriptorSerializer[] serializers = VersionedSerializers.forVersion("1.0",
                                                                                         true).getSerializers(kdvds);
                    KeyHashDecoder decoder = new EntryDataDecoder(null, null, serializers);
                    try {
                        if (results[i] != null) {
                            Cell kv = CellUtils.matchDataColumn(results[i].raw());
                            decoder.set(kv.getRowArray(), kv.getValueOffset(), kv.getValueLength());
                            decoder.decode(row);
                        }
                    } finally {
                        Closeables.closeQuietly(decoder);
                    }
                }
                this.currentRowLocation = new HBaseRowLocation(results[results.length - 1].getRow());
                this.currentRow = row_array[results.length - 1];
                this.currentResult = results[results.length - 1];
                return results.length;
            }
            return 0;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw StandardException.newException("Error during fetchNextGroup " + e);
        }
    }

    public int fetchNextGroup(DataValueDescriptor[][] row_array, RowLocation[] oldrowloc_array, RowLocation[] newrowloc_array) throws StandardException {
        throw new RuntimeException("Not Implemented");
        //	return 0;
    }

    private void logIndexKeys() {
        try {
            if (startKeyValue != null) {
                for (int i = 0; i < startKeyValue.length; i++)
                    LOG.trace("startkey - " + startKeyValue[i].getTypeName() + " : " + startKeyValue[i].getTraceString());
            }
            if (stopKeyValue != null) {
                for (int i = 0; i < stopKeyValue.length; i++)
                    LOG.trace("stopKey - " + stopKeyValue[i].getTypeName() + " : " + stopKeyValue[i].getTraceString());
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
        if (table == null)
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
