package com.splicemachine.derby.impl.store.access.base;

import java.io.IOException;
import java.util.Properties;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.FormatableBitSetUtils;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.SpaceInfo;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.derby.impl.store.raw.data.SpaceInformation;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

public abstract class SpliceController implements ConglomerateController {
    protected static Logger LOG = Logger.getLogger(SpliceController.class);
    protected OpenSpliceConglomerate openSpliceConglomerate;
    protected Transaction trans;
    protected String transID;
    //		protected EntryEncoder entryEncoder;
    protected EntryDataHash entryEncoder;
    private String tableVersion;

    public SpliceController() {
    }

    public SpliceController(OpenSpliceConglomerate openSpliceConglomerate, Transaction trans) {
        this.openSpliceConglomerate = openSpliceConglomerate;
        try {
            ((SpliceTransaction) trans).setActiveState(false, false, false, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.trans = trans;
        this.transID = SpliceUtils.getTransID(trans);
        this.tableVersion = "1.0";    //TODO -sf- move this to non-1.0
    }

    public void close() throws StandardException {
        Closeables.closeQuietly(entryEncoder);
        try {
            if ((openSpliceConglomerate != null) && (openSpliceConglomerate.getTransactionManager() != null))
                openSpliceConglomerate.getTransactionManager().closeMe(this);
        } catch (Exception e) {
            throw StandardException.newException("error on close" + e);
        }
    }

    public void getTableProperties(Properties prop) throws StandardException {
        SpliceLogUtils.trace(LOG, "getTableProperties: %s", prop);
    }

    public Properties getInternalTablePropertySet(Properties prop) throws StandardException {
        SpliceLogUtils.trace(LOG, "getInternalTablePropertySet: %s", prop);
        return prop;
    }

    public boolean closeForEndTransaction(boolean closeHeldScan) throws StandardException {
        SpliceLogUtils.trace(LOG, "closeForEndTransaction:");
        return false;
    }

    public void checkConsistency() throws StandardException {
        if (LOG.isTraceEnabled())
            LOG.trace("checkConsistency: (Not Implemented)");
    }

    public boolean delete(RowLocation loc) throws StandardException {
        HTableInterface htable = SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
        try {
            SpliceUtils.doDelete(htable, transID, loc.getBytes());
            return true;
        } catch (Exception e) {
            throw StandardException.newException("delete Failed", e);
        } finally {
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG, "Unable to close HTable");
            }
        }
    }

    public boolean fetch(RowLocation loc, DataValueDescriptor[] destRow, FormatableBitSet validColumns) throws StandardException {
        return fetch(loc, destRow, validColumns, false);
    }

    public boolean lockRow(RowLocation loc, int lock_oper, boolean wait, int lock_duration) throws StandardException {
        if (LOG.isTraceEnabled())
            LOG.trace("lock row: (Not Enabled)");
        return true;
    }

    public boolean lockRow(long page_num, int record_id, int lock_oper, boolean wait, int lock_duration) throws StandardException {
        if (LOG.isTraceEnabled())
            LOG.trace("lock row: (Not Enabled)");
        return true;
    }

    public void unlockRowAfterRead(RowLocation loc, boolean forUpdate, boolean row_qualified) throws StandardException {
        if (LOG.isTraceEnabled())
            LOG.trace("unlockRowAfterReady: (Not Enabled)");
    }

    public RowLocation newRowLocationTemplate() throws StandardException {
//		if (LOG.isTraceEnabled())
//			LOG.trace("newRowLocationTemplate");
        return new HBaseRowLocation();
    }

    public SpaceInfo getSpaceInfo() throws StandardException {
//		if (LOG.isTraceEnabled())
//			LOG.trace("getSpaceInfo: (Not Enabled)");
        return new SpaceInformation(0l, 0l, 0l);
    }

    public void debugConglomerate() throws StandardException {
        if (LOG.isTraceEnabled())
            LOG.trace("debugConglomerate: (Not Enabled)");
    }

    public boolean isKeyed() {
        if (LOG.isTraceEnabled())
            LOG.trace("isKeyed: (Not Enabled)");
        return false;
    }

    public boolean fetch(RowLocation loc, DataValueDescriptor[] destRow, FormatableBitSet validColumns, boolean waitForLock) throws StandardException {
        HTableInterface htable = SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
        try {
            Get get = SpliceUtils.createGet(loc, destRow, validColumns, transID);
            Result result = htable.get(get);
            if (result == null || result.isEmpty()) return false;
            int[] cols = FormatableBitSetUtils.toIntArray(validColumns);
            DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion, true).getSerializers(destRow);
            KeyHashDecoder rowDecoder = new EntryDataDecoder(cols, null, serializers);
//						EntryDecoder decoder = new EntryDecoder(SpliceDriver.getKryoPool());
            ExecRow row = new ValueRow(destRow.length);
            row.setRowArray(destRow);
            row.resetRowArray();
            Cell keyValue = CellUtils.matchDataColumn(result.rawCells());
            rowDecoder.set(keyValue.getValueArray(), keyValue.getValueOffset(), keyValue.getValueLength());
            rowDecoder.decode(row);
            return true;
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        } finally {
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG, "Unable to close HTable");
            }
        }
    }

    @Override
    public String toString() {
        return "SpliceController {conglomId=" + openSpliceConglomerate.getConglomerate().getContainerid() + "}";
    }

    protected HTableInterface getHTable() {
        return SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
    }

    protected void closeHTable(HTableInterface htable) {
        try {
            htable.close();
        } catch (IOException e) {
            SpliceLogUtils.warn(LOG, "Unable to close htable");
        }
    }

    protected void encodeRow(DataValueDescriptor[] row, Put put, int[] columns, FormatableBitSet validColumns) throws StandardException, IOException {
//				BitSet scalarFields = DerbyBytesUtil.getScalarFields(row);
//				BitSet floatFields = DerbyBytesUtil.getFloatFields(row);
//				BitSet doubleFields = DerbyBytesUtil.getDoubleFields(row);
        if (entryEncoder == null) {
//						BitSet nonNullColumns = EncodingUtils.getNonNullColumns(row, validColumns);
            int[] validCols = null;
            validCols = SpliceUtils.bitSetToMap(validColumns);
//						if(columns!=null){
//								validCols = new int[columns.length];
//
//								Arrays.fill(validCols,-1);
//								for(int i=validColumns.anySetBit();i>=0 && i<validCols.length;i=validColumns.anySetBit(i)){
//										validCols[i] = columns[i];
//								}
//						}else if(validColumns!=null){
//								validCols = new int[validColumns.getLength()];
//								Arrays.fill(validCols,-1);
//								for(int i=validColumns.anySetBit();i>=0;i=validColumns.anySetBit(i)){
//										validCols[i] = i;
//								}
//						}
            DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion, true).getSerializers(row);
            entryEncoder = new EntryDataHash(validCols, null, serializers);
        }
//				entryEncoder = EntryEncoder.create(SpliceDriver.getKryoPool(),row.length, nonNullColumns,scalarFields,floatFields,doubleFields);
//				else
//						entryEncoder.reset(nonNullColumns,scalarFields,floatFields,doubleFields);
        ValueRow rowToEncode = new ValueRow(row.length);
        rowToEncode.setRowArray(row);
        entryEncoder.setRow(rowToEncode);
        byte[] data = entryEncoder.encode();
//				EncodingUtils.encodeRow(row, put, columns, validColumns, entryEncoder);
        put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES, data);
    }
}
