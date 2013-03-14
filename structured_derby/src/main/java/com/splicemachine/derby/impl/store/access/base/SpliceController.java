package com.splicemachine.derby.impl.store.access.base;

import java.util.Properties;

import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.SpaceInfo;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.store.raw.data.SpaceInformation;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.constants.TxnConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.SpliceUtils;

public abstract class SpliceController implements ConglomerateController {
	protected static Logger LOG = Logger.getLogger(SpliceController.class);
	protected OpenSpliceConglomerate openSpliceConglomerate;
	protected HTableInterface htable;
	protected Transaction trans;
	protected String transID;
	
	public SpliceController() {}

	public SpliceController(OpenSpliceConglomerate openSpliceConglomerate, Transaction trans) {
		this.openSpliceConglomerate = openSpliceConglomerate;
		try {
			((SpliceTransaction)trans).setActiveState();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.trans = trans;
		this.transID = SpliceUtils.getTransID(trans);
//		if (LOG.isTraceEnabled())
//			LOG.trace("instantiate HBaseControl with openHBase: " + openSpliceConglomerate);
		htable = SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
	}
	
	public void close() throws StandardException {
//		if (LOG.isTraceEnabled())
//			LOG.trace("close:");
		try {
			htable.close();
			if ((openSpliceConglomerate != null) && (openSpliceConglomerate.getTransactionManager() != null))
				openSpliceConglomerate.getTransactionManager().closeMe(this);
		} catch (Exception e) {
			throw StandardException.newException("error on close" + e);
		}
	}
	
	public void getTableProperties(Properties prop) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("getTableProperties: " + prop);		
	}

	
	public Properties getInternalTablePropertySet(Properties prop) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("getInternalTablePropertySet: " + prop);		
		return prop;
	}
	
	public boolean closeForEndTransaction(boolean closeHeldScan) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("closeForEndTransaction:");				
		return false;
	}

	
	public void checkConsistency() throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("checkConsistency: (Not Implemented)");						
	}
	
	
	public boolean delete(RowLocation loc) throws StandardException {
//		if (LOG.isTraceEnabled())
//			LOG.trace("delete row location " + loc.getBytes());
		try {
			Delete delete = new Delete(loc.getBytes());
			if (transID != null) {
                SpliceUtils.getTransactionGetsPuts().prepDelete(transID, delete);
            }
			htable.delete(delete);
			return true;
		} catch (Exception e) {
			throw StandardException.newException("delete Failed", e);
		}
	}
	
	public boolean fetch(RowLocation loc, DataValueDescriptor[] destRow, FormatableBitSet validColumns) throws StandardException {
		return fetch(loc,destRow,validColumns,false);
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

	
	public void unlockRowAfterRead(RowLocation loc, boolean forUpdate,boolean row_qualified) throws StandardException {
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
		return new SpaceInformation(0l,0l,0l);
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
//		if (LOG.isTraceEnabled())
//			LOG.trace("fetch rowlocation " + loc + ", destRow " + destRow + ", validColumns " + validColumns + ", waitForLock " + waitForLock);
		try {
			Get get = SpliceUtils.createGet(loc, destRow, validColumns, transID);
			Result result = htable.get(get);
            if(result==null||result.isEmpty()) return false;
			SpliceUtils.populate(result, validColumns, destRow);	
			return true;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
    }


	@Override
	public String toString() {
		return "SpliceController {conglomId="+Bytes.toString(htable.getTableName())+"}";
	}
	
	
}
