package com.splicemachine.derby.impl.store;

import com.carrotsearch.hppc.BitSet;
import com.google.common.io.Closeables;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.storage.ByteEntryAccumulator;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.HConstants;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 3/11/14
 */
public class ExecRowAccumulator extends ByteEntryAccumulator {
		protected final DataValueDescriptor[] dvds;
		protected final DescriptorSerializer[] serializers;
		protected final int[] columnMap;

		private ExecRowAccumulator(EntryPredicateFilter predicateFilter,
															boolean returnIndex,
															BitSet fieldsToCollect,
															DataValueDescriptor[] dvds,
															int[] columnMap,
															DescriptorSerializer[] serializers) {
				super(predicateFilter, returnIndex, fieldsToCollect);
				this.dvds = dvds;
				this.columnMap = columnMap;
				this.serializers = serializers;
		}

		public static ExecRowAccumulator newAccumulator(EntryPredicateFilter predicateFilter,
																										boolean returnIndex,
																										ExecRow row,
																										int[] columnMap,
																										boolean[] columnSortOrder,
																										FormatableBitSet cols,
																										String tableVersion){
				DataValueDescriptor[] dvds = row.getRowArray();
				BitSet fieldsToCollect = new BitSet(dvds.length);
				if(cols!=null){
						for(int i=cols.anySetBit();i>=0;i=cols.anySetBit(i))
								fieldsToCollect.set(i);
				}else if(columnMap!=null){
						for(int i=0;i<columnMap.length;i++){
								int pos = columnMap[i];
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
				DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion,false).getSerializers(row);
				if(columnSortOrder!=null)
						return new Ordered(predicateFilter,returnIndex,fieldsToCollect,dvds,columnMap,serializers,columnSortOrder);
				else
						return new ExecRowAccumulator(predicateFilter,returnIndex,fieldsToCollect,dvds,columnMap,serializers);
		}

		public static ExecRowAccumulator newAccumulator(EntryPredicateFilter predicateFilter,
																										boolean returnIndex,
																										ExecRow row,
																										int[] columnMap,
																										FormatableBitSet cols,
																										String tableVersion){
				return newAccumulator(predicateFilter,returnIndex,row,columnMap,null,cols,tableVersion);
		}

		public static ExecRowAccumulator newAccumulator(EntryPredicateFilter predicateFilter,
																										boolean returnIndex,
																										ExecRow row,
																										int[] keyColumns,
																										String tableVersion){
				return newAccumulator(predicateFilter,returnIndex,row,keyColumns,null,tableVersion);
		}

		@Override
		protected void occupy(int position, byte[] data, int offset, int length) {
				decode(position, data, offset, length);
				super.occupy(position,data,offset,length);
		}

		@Override
		protected void occupyDouble(int position, byte[] data, int offset, int length) {
				decode(position, data, offset, length);
				super.occupyDouble(position, data, offset, length);
		}

		@Override
		protected void occupyFloat(int position, byte[] data, int offset, int length) {
				decode(position, data, offset, length);
				super.occupyFloat(position, data, offset, length);
		}

		@Override
		protected void occupyScalar(int position, byte[] data, int offset, int length) {
				decode(position,data,offset,length);
				super.occupyScalar(position, data, offset, length);
		}

		@Override
		public byte[] finish() {
				if(checkFilterAfter()) return null;
				return HConstants.EMPTY_BYTE_ARRAY;
		}

		protected void decode(int position, byte[] data, int offset, int length) {
				DataValueDescriptor dvd = dvds[columnMap[position]];
				DescriptorSerializer serializer = serializers[columnMap[position]];
				try {
						serializer.decodeDirect(dvd, data, offset, length, false);
				} catch (StandardException e) {
						//TODO -sf- handle this?
						throw new RuntimeException(e);
				}
		}

		public void close() {
				for(DescriptorSerializer serializer:serializers){
						Closeables.closeQuietly(serializer);
				}
		}

		private static class Ordered extends ExecRowAccumulator{

				private final boolean[] columnSortOrder;

				private Ordered(EntryPredicateFilter predicateFilter,
												boolean returnIndex,
												BitSet fieldsToCollect,
												DataValueDescriptor[] dvds,
												int[] columnMap,
												DescriptorSerializer[] serializers,
												boolean[] columnSortOrder) {
						super(predicateFilter, returnIndex, fieldsToCollect, dvds, columnMap, serializers);
						this.columnSortOrder = columnSortOrder;
				}

				@Override
				protected void decode(int position, byte[] data, int offset, int length) {
						DataValueDescriptor dvd = dvds[columnMap[position]];
						DescriptorSerializer serializer = serializers[columnMap[position]];
						try {
								serializer.decodeDirect(dvd, data, offset, length, !columnSortOrder[position]);
						} catch (StandardException e) {
								//TODO -sf- handle this?
								throw new RuntimeException(e);
						}
				}
		}
}
