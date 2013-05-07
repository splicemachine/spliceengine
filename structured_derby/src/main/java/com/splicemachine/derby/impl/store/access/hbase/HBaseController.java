package com.splicemachine.derby.impl.store.access.hbase;

import com.google.common.io.Closeables;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceController;
import com.splicemachine.derby.utils.SpliceUtils;

import java.io.IOException;


public class HBaseController  extends SpliceController {
	protected static Logger LOG = Logger.getLogger(HBaseController.class);

	public HBaseController() {
		super();
	}

	public HBaseController(OpenSpliceConglomerate openSpliceConglomerate, Transaction trans) {
		super(openSpliceConglomerate, trans);
	}

	@Override
	public int insert(DataValueDescriptor[] row) throws StandardException {
        SpliceLogUtils.trace(LOG,"insert into conglom %d row %s with txnId %s",openSpliceConglomerate.getConglomerate().getContainerid(),row,transID);
        HTableInterface htable = SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
		try {
			htable.put(Puts.buildInsert(row, transID));
			return 0;
		} catch (Exception e) {
			LOG.error(e.getMessage(),e);
		}finally{
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG,"Unable to close htable");
            }
        }
		return -1;
	}

	@Override
	public void insertAndFetchLocation(DataValueDescriptor[] row,
			RowLocation destRowLocation) throws StandardException {
        SpliceLogUtils.trace(LOG,"insertAndFetchLocation into conglom %d row %s",openSpliceConglomerate.getConglomerate().getContainerid(),row);
        HTableInterface htable = SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
		try {
			Put put = Puts.buildInsert(row, transID);
			destRowLocation.setValue(put.getRow());
			if (LOG.isTraceEnabled())
				LOG.trace("insertAndFetchLocation returned rowlocation " + destRowLocation.getBytes());	
			htable.put(put);
		} catch (Exception e) {
			throw StandardException.newException("insert and fetch location error",e);
		} finally{
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG,"Unable to close HTable");
            }
        }
	}

	@Override
	public boolean replace(RowLocation loc, DataValueDescriptor[] row, FormatableBitSet validColumns) throws StandardException {
        SpliceLogUtils.trace(LOG,"replace rowLocation %s, destRow %s, validColumns %s",loc,row,validColumns);
        HTableInterface htable = SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
		try {
			htable.put(Puts.buildInsert(loc.getBytes(),row, validColumns, transID));
			return true;			
		} catch (Exception e) {
			throw StandardException.newException("Error during replace " + e);
		} finally{
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG,"Unable to close HTable");
            }
        }
	}

}
