package com.splicemachine.derby.impl.store;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.storage.ByteEntryAccumulator;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.HConstants;

/**
 * @author Scott Fines
 * Date: 3/11/14
 */
public class ExecRowAccumulator extends ByteEntryAccumulator {
		private final DataValueDescriptor[] dvds;
		private final int[] columnMap;

		private ExecRowAccumulator(EntryPredicateFilter predicateFilter,
															boolean returnIndex,
															BitSet fieldsToCollect,
															DataValueDescriptor[] dvds,
															int[] columnMap) {
				super(predicateFilter, returnIndex, fieldsToCollect);
				this.dvds = dvds;
				this.columnMap = columnMap;
		}

		public static ExecRowAccumulator newAccumulator(EntryPredicateFilter predicateFilter,
																										boolean returnIndex,
																										ExecRow row,
																										int[] keyColumns){
				DataValueDescriptor[] dvds = row.getRowArray();
				BitSet fieldsToCollect = new BitSet(dvds.length);
				if(keyColumns!=null){
						for(int i=0;i<keyColumns.length;i++){
								int pos = keyColumns[i];
								if(pos<0) continue;
								if(dvds[pos]!=null)
										fieldsToCollect.set(i);
						}
				}else{
						for(int i=0;i<dvds.length;i++){
								if(dvds[i]!=null)
										fieldsToCollect.set(i);
						}
				}
				return new ExecRowAccumulator(predicateFilter,returnIndex,fieldsToCollect,dvds,keyColumns);
		}

		public static ExecRowAccumulator newAccumulator(EntryPredicateFilter predicateFilter,
																										boolean returnIndex,
																										ExecRow row){
				return newAccumulator(predicateFilter,returnIndex,row,null);
		}

		@Override
		protected void occupy(int position, byte[] data, int offset, int length) {
				DataValueDescriptor dvd = dvds[columnMap[position]];
				try {
						DerbyBytesUtil.decode(dvd, data, offset, length);
				} catch (StandardException e) {
						//TODO -sf- handle this?
						throw new RuntimeException(e);
				}
				super.occupy(position,data,offset,length);
		}

		@Override
		public byte[] finish() {
				if(checkFilterAfter()) return null;
				return HConstants.EMPTY_BYTE_ARRAY;
		}
}
