package com.splicemachine.derby.impl.store.access.hbase;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceController;
import com.splicemachine.derby.utils.SpliceUtils;


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
		if (LOG.isTraceEnabled())
			LOG.trace("insert into conglom " + Bytes.toString(this.htable.getTableName()) + " row " + row+", with transID="+transID);	
		try {
			htable.put(SpliceUtils.insert(row, transID));
			return 0;
		} catch (Exception e) {
			LOG.error(e.getMessage(),e);
		}
		return -1;
	}

	@Override
	public void insertAndFetchLocation(DataValueDescriptor[] row,
			RowLocation destRowLocation) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("insertAndFetchLocation into conglom " + Bytes.toString(this.htable.getTableName()) + " row " + row);	
		try {
			Put put = SpliceUtils.insert(row, transID);
			destRowLocation.setValue(put.getRow());
			if (LOG.isTraceEnabled())
				LOG.trace("insertAndFetchLocation returned rowlocation " + destRowLocation.getBytes());	
			htable.put(put);
		} catch (Exception e) {
			throw StandardException.newException("insert and fetch location error",e);
		}	
	}

	@Override
	public boolean replace(RowLocation loc, DataValueDescriptor[] row, FormatableBitSet validColumns) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("replace rowlocation " + loc + ", destRow " + row + ", validColumns " + validColumns);
		try {
			htable.put(SpliceUtils.insert(row, validColumns, loc.getBytes(), transID));
			return true;			
		} catch (Exception e) {
			throw StandardException.newException("Error during replace " + e);
		}
	}

}
