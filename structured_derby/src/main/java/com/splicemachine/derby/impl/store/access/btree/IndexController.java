
package com.splicemachine.derby.impl.store.access.btree;

import java.io.IOException;

import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceController;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;


public class IndexController  extends SpliceController  {
	private static Logger LOG = Logger.getLogger(IndexController.class);
	private int nKeyFields;

	public IndexController() {
		super();
	}

	public IndexController(OpenSpliceConglomerate openSpliceConglomerate, Transaction trans, int nKeyFields) {
		super(openSpliceConglomerate, trans);
		this.nKeyFields = nKeyFields;
	}

	private byte[] generateIndexKey(DataValueDescriptor[] row, boolean[] order) throws IOException, StandardException {
		if (row.length == nKeyFields) {
			return DerbyBytesUtil.generateIndexKey(row,order);
		}
		DataValueDescriptor[] uniqueRow = new DataValueDescriptor[nKeyFields];
		System.arraycopy(row, 0, uniqueRow, 0, nKeyFields);
		return DerbyBytesUtil.generateIndexKey(uniqueRow,order);
	}

	@Override
	public int insert(DataValueDescriptor[] row) throws StandardException {
        SpliceLogUtils.trace(LOG,"insert row");
        HTableInterface htable = getHTable();
		try {
			boolean[] order = ((IndexConglomerate)this.openSpliceConglomerate.getConglomerate()).getAscDescInfo();
			byte[] rowKey = generateIndexKey(row, order);
            Put put = SpliceUtils.createPut(rowKey,transID);
            RowMarshaller.denseColumnar().encodeRow(row, null, put, null);
			htable.put(put);
			return 0;
		} catch (Exception e) {
			LOG.error(e.getMessage(),e);
			throw StandardException.plainWrapException(e);
		}finally{
           closeHTable(htable);
        }
	}

	@Override
	public void insertAndFetchLocation(DataValueDescriptor[] row,RowLocation destRowLocation) throws StandardException {
		SpliceLogUtils.trace(LOG, "insertAndFetchLocation rowLocation %s",destRowLocation);
        HTableInterface htable = getHTable();
		try {
			boolean[] order = ((IndexConglomerate)this.openSpliceConglomerate.getConglomerate()).getAscDescInfo();
			byte[] rowKey = generateIndexKey(row, order);

            Put put = SpliceUtils.createPut(rowKey,transID);
            RowMarshaller.denseColumnar().encodeRow(row, null, put, null);

			destRowLocation.setValue(put.getRow());
			htable.put(put);
		} catch (Exception e) {
			throw StandardException.newException("insert and fetch location error",e);
		} finally{
            closeHTable(htable);
        }
	}

	@Override
	public boolean replace(RowLocation loc, DataValueDescriptor[] row, FormatableBitSet validColumns) throws StandardException {
		SpliceLogUtils.trace(LOG, "replace rowlocation %s, destRow %s, validColumns ", loc, row, validColumns);
        HTableInterface htable = getHTable();
		try {
			boolean[] sortOrder = ((IndexConglomerate) this.openSpliceConglomerate.getConglomerate()).getAscDescInfo();
			if (openSpliceConglomerate.cloneRowTemplate().length == row.length && validColumns == null) {
                Put put = SpliceUtils.createPut(generateIndexKey(row,sortOrder),transID);
                RowMarshaller.columnar().encodeRow(row, SpliceUtils.bitSetToMap(validColumns), put, null);
                htable.put(put);
			} else {
				DataValueDescriptor[] oldValues = openSpliceConglomerate.cloneRowTemplate();
				Get get = SpliceUtils.createGet(loc, oldValues, null, transID);
				Result result = htable.get(get);
                for(KeyValue kv:result.raw()){
                    RowMarshaller.columnar().decode(kv, oldValues, null, null);
                }
                int[] validCols = new int[validColumns.getNumBitsSet()];
                int pos=0;
                for(int i=validColumns.anySetBit();i!=-1;i=validColumns.anySetBit(i)){
                    oldValues[i] = row[i];
                    validCols[pos] = i;
                }
                byte[] rowKey = generateIndexKey(row,sortOrder);
                Put put = SpliceUtils.createPut(rowKey,transID);
                RowMarshaller.columnar().encodeRow(row, validCols, put, null);
                htable.put(put);
			}
			super.delete(loc);
			return true;			
		} catch (Exception e) {
			throw StandardException.newException("Error during replace " + e);
		} finally{
            closeHTable(htable);
        }

	}

	@Override
	public boolean isKeyed() {
		return true;
	}

}
