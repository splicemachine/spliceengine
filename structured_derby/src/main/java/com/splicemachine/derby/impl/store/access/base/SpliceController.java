package com.splicemachine.derby.impl.store.access.base;

import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.SpaceInfo;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.store.raw.data.SpaceInformation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

public abstract class SpliceController implements ConglomerateController {
	protected static Logger LOG = Logger.getLogger(SpliceController.class);
	protected OpenSpliceConglomerate openSpliceConglomerate;
	protected Transaction trans;
	protected String transID;
	
	public SpliceController() {}

	public SpliceController(OpenSpliceConglomerate openSpliceConglomerate, Transaction trans) {
		this.openSpliceConglomerate = openSpliceConglomerate;
		try {
			((SpliceTransaction)trans).setActiveState(false, false, false, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.trans = trans;
		this.transID = SpliceUtils.getTransID(trans);
	}
	
	public void close() throws StandardException {
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
		SpliceLogUtils.trace(LOG,"closeForEndTransaction:");				
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
		}finally{
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG,"Unable to close HTable");
            }
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
        HTableInterface htable = SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
		try {
			Get get = SpliceUtils.createGet(loc, destRow, validColumns, transID);
			Result result = htable.get(get);
            if(result==null||result.isEmpty()) return false;
            for(KeyValue kv:result.raw()){
                MultiFieldDecoder decoder = MultiFieldDecoder.create();
                RowMarshaller.packed().decode(kv, destRow, null, decoder);
            }
			return true;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}finally{
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG,"Unable to close HTable");
            }
        }
    }


	@Override
	public String toString() {
		return "SpliceController {conglomId="+openSpliceConglomerate.getConglomerate().getContainerid()+"}";
	}

    protected HTableInterface getHTable(){
        return SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
    }

    protected void closeHTable(HTableInterface htable){
        try {
            htable.close();
        } catch (IOException e) {
            SpliceLogUtils.warn(LOG,"Unable to close htable");
        }
    }
	
}
