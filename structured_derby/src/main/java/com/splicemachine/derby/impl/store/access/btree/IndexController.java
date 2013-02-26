
package com.splicemachine.derby.impl.store.access.btree;

import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.utils.Puts;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceController;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;


public class IndexController  extends SpliceController  {
	private static Logger LOG = Logger.getLogger(IndexController.class);

	public IndexController() {
		super();
	}

	public IndexController(OpenSpliceConglomerate openSpliceConglomerate, Transaction trans) {
		super(openSpliceConglomerate, trans);
	}


	@Override
	public int insert(DataValueDescriptor[] row) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("insert " + row + ", row ");	
		try {
			boolean[] order = ((IndexConglomerate)this.openSpliceConglomerate.getConglomerate()).getAscDescInfo();
			byte[] rowKey = DerbyBytesUtil.generateIndexKey(row,order);
			htable.put(Puts.buildInsert(rowKey, row, transID,new Serializer()));
//			htable.put(SpliceUtils.insert(row,DerbyBytesUtil.generateIndexKey(row,((IndexConglomerate) this.openSpliceConglomerate.getConglomerate()).getAscDescInfo()),  transID));
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
			LOG.trace("insertAndFetchLocation row " + row);	
		try {
			boolean[] order = ((IndexConglomerate)this.openSpliceConglomerate.getConglomerate()).getAscDescInfo();
			byte[] rowKey = DerbyBytesUtil.generateIndexKey(row,order);
			Put put = Puts.buildInsert(rowKey,row,transID,new Serializer());
//			Put put = SpliceUtils.insert(row,DerbyBytesUtil.generateIndexKey(row,((IndexConglomerate) this.openSpliceConglomerate.getConglomerate()).getAscDescInfo()), transID);
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
			boolean[] sortOrder = ((IndexConglomerate) this.openSpliceConglomerate.getConglomerate()).getAscDescInfo();
			if (openSpliceConglomerate.cloneRowTemplate().length == row.length && validColumns == null) {
				htable.put(Puts.buildInsert(DerbyBytesUtil.generateIndexKey(row,sortOrder),row,validColumns,transID));
//				htable.put(SpliceUtils.insert(row, validColumns,DerbyBytesUtil.generateIndexKey(row,sortOrder), transID));
			} else {
				DataValueDescriptor[] oldValues = openSpliceConglomerate.cloneRowTemplate();
				Get get = SpliceUtils.createGet(loc, oldValues, null, transID);
				Result result = htable.get(get);
				SpliceUtils.populate(result, null, oldValues);	
				for (int i =0;i<row.length;i++) {
					if (validColumns.isSet(i))
						oldValues[i] = row[i];
				}
				htable.put(Puts.buildInsert(DerbyBytesUtil.generateIndexKey(row,sortOrder),row,validColumns, transID));
			}
			super.delete(loc);
			return true;			
		} catch (Exception e) {
			throw StandardException.newException("Error during replace " + e);
		}
	}

	@Override
	public boolean isKeyed() {
		return true;
	}

}
