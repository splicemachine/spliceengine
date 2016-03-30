package com.splicemachine.derby.impl.store.access.base;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.FormatableBitSetUtils;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.SpaceInfo;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.impl.store.raw.data.SpaceInformation;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Properties;

public abstract class SpliceController<Data> implements ConglomerateController {
		protected static final SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();
		protected static Logger LOG = Logger.getLogger(SpliceController.class);
		protected OpenSpliceConglomerate openSpliceConglomerate;
		protected BaseSpliceTransaction trans;
		protected EntryDataHash entryEncoder;
		private String tableVersion;
    private HTableInterface table;

    public SpliceController() {}

    public SpliceController(OpenSpliceConglomerate openSpliceConglomerate, Transaction trans) {
        this.openSpliceConglomerate = openSpliceConglomerate;
        this.trans = (BaseSpliceTransaction)trans;
        try {
            this.trans.setActiveState(false, false, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.tableVersion = "1.0";	//TODO -sf- move this to non-1.0
    }

    public void close() throws StandardException {
        if(table!=null){
            try{
                table.close();
            }catch(IOException ioe){
                throw Exceptions.parseException(ioe);
            }
        }
        Closeables.closeQuietly(table);
        Closeables.closeQuietly(entryEncoder);
        try {
            if ((openSpliceConglomerate != null) && (openSpliceConglomerate.getTransactionManager() != null))
                openSpliceConglomerate.getTransactionManager().closeMe(this);
        } catch (Exception e) {
            throw StandardException.newException("error on close" + e);
        }
    }

    public void getTableProperties(Properties prop) throws StandardException { }
		public Properties getInternalTablePropertySet(Properties prop) throws StandardException { return prop; }
		public boolean closeForEndTransaction(boolean closeHeldScan) throws StandardException { return false; }

		public void checkConsistency() throws StandardException {throw new UnsupportedOperationException("checkConsistency not enabled");}

    public boolean lockRow(RowLocation loc, int lock_oper, boolean wait, int lock_duration) throws StandardException {
        throw new UnsupportedOperationException("Unable to lock rows in SpliceMachine");
    }


    public boolean lockRow(long page_num, int record_id, int lock_oper, boolean wait, int lock_duration) throws StandardException {
        throw new UnsupportedOperationException("Unable to lock rows in SpliceMachine");
    }


    public void unlockRowAfterRead(RowLocation loc, boolean forUpdate,boolean row_qualified) throws StandardException {  }


    public RowLocation newRowLocationTemplate() throws StandardException { return new HBaseRowLocation(); }
    public SpaceInfo getSpaceInfo() throws StandardException { return new SpaceInformation(0l,0l,0l); }


    public void debugConglomerate() throws StandardException {
        throw new UnsupportedOperationException("DebugConglomerate Not enabled");
    }

    public boolean isKeyed() { return false; }

		public boolean delete(RowLocation loc) throws StandardException {
				HTableInterface htable = getTable();
				try {
						SpliceUtils.doDelete(htable, ((SpliceTransaction)trans).getTxn(), loc.getBytes());
						return true;
				} catch (Exception e) {
            throw Exceptions.parseException(e);
				}
		}

    public boolean fetch(RowLocation loc, DataValueDescriptor[] destRow, FormatableBitSet validColumns) throws StandardException {
        return fetch(loc,destRow,validColumns,false);
    }

		public boolean fetch(RowLocation loc, DataValueDescriptor[] destRow, FormatableBitSet validColumns, boolean waitForLock) throws StandardException {
				HTableInterface htable = getTable();
                KeyHashDecoder rowDecoder = null;
                try {
						Get get = SpliceUtils.createGet(loc, destRow, validColumns, trans.getTxnInformation());
						Result result = htable.get(get);
						if(result==null||result.isEmpty()) return false;
						int[] cols = FormatableBitSetUtils.toIntArray(validColumns);
						DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(destRow);
						rowDecoder = new EntryDataDecoder(cols,null,serializers);
						ExecRow row = new ValueRow(destRow.length);
						row.setRowArray(destRow);
						row.resetRowArray();
						Object keyValue = dataLib.matchDataColumn(result);						
						rowDecoder.set(dataLib.getDataValueBuffer(keyValue), dataLib.getDataValueOffset(keyValue), dataLib.getDataValuelength(keyValue));
						rowDecoder.decode(row);
						return true;
				} catch (Exception e) {
						throw Exceptions.parseException(e);
				} finally {
                    if (rowDecoder != null) {
                        try {
                            rowDecoder.close();
                        } catch (Exception e) {
                        }
                    }
                }
            }

		@Override
		public String toString() {
				return "SpliceController {conglomId="+openSpliceConglomerate.getConglomerate().getContainerid()+"}";
		}

		protected HTableInterface getTable(){
        if(table==null)
            table =  SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
        return table;
		}


		protected void encodeRow(DataValueDescriptor[] row, Put put,int[] columns,FormatableBitSet validColumns) throws StandardException, IOException {
				if(entryEncoder==null){
						int[] validCols = SpliceUtils.bitSetToMap(validColumns);
						DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(row);
						entryEncoder = new EntryDataHash(validCols,null,serializers);
				}
				ValueRow rowToEncode = new ValueRow(row.length);
				rowToEncode.setRowArray(row);
				entryEncoder.setRow(rowToEncode);
				byte[] data = entryEncoder.encode();
				put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES,trans.getActiveStateTxn().getTxnId(),data);
		}

    protected void elevateTransaction() throws StandardException {
        ((SpliceTransaction)trans).elevate(Bytes.toBytes(Long.toString(openSpliceConglomerate.getConglomerate().getContainerid())));
    }

    public SpliceConglomerate getConglomerate() {
        return (SpliceConglomerate)openSpliceConglomerate.getConglomerate();
    }
}
