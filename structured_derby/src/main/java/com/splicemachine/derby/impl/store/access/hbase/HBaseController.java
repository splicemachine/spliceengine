package com.splicemachine.derby.impl.store.access.hbase;

import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceController;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

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
            Put put = SpliceUtils.createPut(SpliceUtils.getUniqueKey(),transID);

            encodeRow(row,put,null,null);
            htable.put(put);
			return 0;
		} catch (Exception e) {
            throw Exceptions.parseException(e);
//            LOG.error(e.getMessage(),e);
		}finally{
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.warn(LOG,"Unable to close htable");
            }
        }
	}

	@Override
	public void insertAndFetchLocation(DataValueDescriptor[] row,
			RowLocation destRowLocation) throws StandardException {
        SpliceLogUtils.trace(LOG,"insertAndFetchLocation into conglom %d row %s",openSpliceConglomerate.getConglomerate().getContainerid(),row);
        HTableInterface htable = SpliceAccessManager.getHTable(openSpliceConglomerate.getConglomerate().getContainerid());
		try {
            Put put = SpliceUtils.createPut(SpliceUtils.getUniqueKey(), transID);
            encodeRow(row, put, null, null);

			destRowLocation.setValue(put.getRow());
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
            Put put = SpliceUtils.createPut(loc.getBytes(),transID);

            encodeRow(row, put, SpliceUtils.bitSetToMap(validColumns), validColumns);
            htable.put(put);
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
