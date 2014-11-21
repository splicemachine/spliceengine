
package com.splicemachine.derby.impl.store.access.btree;

import java.io.IOException;
import java.util.Arrays;

import com.google.common.io.Closeables;
import com.splicemachine.derby.impl.storage.KeyValueUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceController;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;


public class IndexController  extends SpliceController  {
		private static Logger LOG = Logger.getLogger(IndexController.class);
		private int nKeyFields;

		public IndexController(OpenSpliceConglomerate openSpliceConglomerate, Transaction trans, int nKeyFields) {
				super(openSpliceConglomerate, trans);
				this.nKeyFields = nKeyFields;
		}

		private byte[] generateIndexKey(DataValueDescriptor[] row, boolean[] order) throws IOException, StandardException {
				if (row.length == nKeyFields) {
						return DerbyBytesUtil.generateIndexKey(row,order,"1.0");
				}
				DataValueDescriptor[] uniqueRow = new DataValueDescriptor[nKeyFields];
				System.arraycopy(row, 0, uniqueRow, 0, nKeyFields);
				return DerbyBytesUtil.generateIndexKey(uniqueRow,order,"1.0");
		}

    public int nKeyFields(){
        return nKeyFields;
    }

		@Override
		public int insert(DataValueDescriptor[] row) throws StandardException {
				if (LOG.isTraceEnabled()) LOG.trace(String.format("insert row into conglomerate: %s, row: %s", this.getConglomerate(), (row==null ? null : Arrays.toString(row))));
				HTableInterface htable = getTable();
				try {
						boolean[] order = ((IndexConglomerate)this.openSpliceConglomerate.getConglomerate()).getAscDescInfo();
						byte[] rowKey = generateIndexKey(row, order);
						/*
						 * Check if the rowKey already exists.
						 * TODO: An optimization would be to not check for existence of a rowKey if the index is non-unique.
						 *		 Unfortunately, this information is not available here and would need to be passed down from
						 *		 DataDictionaryImpl through TabInfoImpl.  Something worth looking into in the future.
						 */
						Get get = SpliceUtils.createGet(((SpliceTransaction)trans).getTxn(), rowKey);
						Result result = htable.get(get);
						if (result.isEmpty()) {
							Put put = SpliceUtils.createPut(rowKey,((SpliceTransaction)trans).getTxn());
							encodeRow(row, put,null,null);
							htable.put(put);
							return 0;
						} else {
							return ConglomerateController.ROWISDUPLICATE;
						}
				} catch (Exception e) {
						LOG.error(e.getMessage(),e);
						throw Exceptions.parseException(e);
				}
		}

		@Override
		public void insertAndFetchLocation(DataValueDescriptor[] row,RowLocation destRowLocation) throws StandardException {
				if (LOG.isTraceEnabled()) LOG.trace(String.format("insertAndFetchLocation into conglomerate: %s, row: %s, rowLocation: %s", this.getConglomerate(), (row==null ? null : Arrays.toString(row)), destRowLocation));
				HTableInterface htable = getTable();
				try {
						boolean[] order = ((IndexConglomerate)this.openSpliceConglomerate.getConglomerate()).getAscDescInfo();
						byte[] rowKey = generateIndexKey(row, order);
						Put put = SpliceUtils.createPut(rowKey,((SpliceTransaction)trans).getTxn());
						encodeRow(row, put,null,null);

						destRowLocation.setValue(put.getRow());
						htable.put(put);
				} catch (Exception e) {
						throw StandardException.newException("insert and fetch location error",e);
				}
		}

		@Override
		public boolean replace(RowLocation loc, DataValueDescriptor[] row, FormatableBitSet validColumns) throws StandardException {
				if (LOG.isTraceEnabled()) LOG.trace(String.format("replace conglomerate: %s, rowlocation: %s, destRow: %s, validColumns: %s", this.getConglomerate(), loc, (row==null ? null : Arrays.toString(row)), validColumns));
				HTableInterface htable = getTable();
				try {
						boolean[] sortOrder = ((IndexConglomerate) this.openSpliceConglomerate.getConglomerate()).getAscDescInfo();
						Put put;
						int[] validCols;
						if (openSpliceConglomerate.cloneRowTemplate().length == row.length && validColumns == null) {
								put = SpliceUtils.createPut(DerbyBytesUtil.generateIndexKey(row,sortOrder,"1.0"),((SpliceTransaction)trans).getTxn());
								validCols = null;
						} else {
								DataValueDescriptor[] oldValues = openSpliceConglomerate.cloneRowTemplate();
								Get get = SpliceUtils.createGet(loc, oldValues, null, trans.getTxnInformation());
								Result result = htable.get(get);
								ExecRow execRow = new ValueRow(oldValues.length);
								execRow.setRowArray(oldValues);
								DescriptorSerializer[] serializers = VersionedSerializers.forVersion("1.0",true).getSerializers(execRow);
								KeyHashDecoder decoder = BareKeyHash.decoder(null,null,serializers);
								try{
										Object kv = dataLib.matchDataColumn(result.raw());
										decoder.set(dataLib.getDataValueBuffer(kv),
												dataLib.getDataValueOffset(kv),
												dataLib.getDataValuelength(kv));
										decoder.decode(execRow);
										validCols = new int[validColumns.getNumBitsSet()];
										int pos=0;
										for(int i=validColumns.anySetBit();i!=-1;i=validColumns.anySetBit(i)){
												oldValues[i] = row[i];
												validCols[pos] = i;
										}
										byte[] rowKey = generateIndexKey(row,sortOrder);
										put = SpliceUtils.createPut(rowKey,((SpliceTransaction)trans).getTxn());
								}finally{
										Closeables.closeQuietly(decoder);
								}
						}

						encodeRow(row,put,validCols,validColumns);
						htable.put(put);
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
