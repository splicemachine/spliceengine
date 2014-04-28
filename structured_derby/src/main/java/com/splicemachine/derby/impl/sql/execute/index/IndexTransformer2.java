package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.storage.*;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Scott Fines
 *         Date: 4/17/14
 */
public class IndexTransformer2 {

		private ByteEntryAccumulator destKeyAccumulator;
		private EntryEncoder destRowEncoder;

		private final boolean isUnique;
		private final boolean isUniqueWithDuplicateNulls;

		private int[] sourceKeyColumnEncodingOrder;
		private int[] columnTypes;
		private boolean[] sourceKeyColumnSortOrder;

		private int[] destKeyEncodingMap;
		private boolean[] destKeyColumnSortOrder;

		private EntryDecoder rowDecoder;
		private MultiFieldDecoder keyDecoder;

		private TypeProvider typeProvider;

		public IndexTransformer2(boolean isUnique,
														 boolean isUniqueWithDuplicateNulls,
														 String tableVersion,
														 int[] sourceKeyColumnEncodingOrder,
														 int[] columnTypes,
														 boolean[] sourceKeyColumnSortOrder,
														 int[] destKeyEncodingMap,
														 boolean[] destKeyColumnSortOrder) {
				this.isUnique = isUnique;
				this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
				this.sourceKeyColumnEncodingOrder = sourceKeyColumnEncodingOrder;
				this.columnTypes = columnTypes;
				this.sourceKeyColumnSortOrder = sourceKeyColumnSortOrder;
				this.destKeyEncodingMap = destKeyEncodingMap;
				this.destKeyColumnSortOrder = destKeyColumnSortOrder;
				this.typeProvider= VersionedSerializers.typesForVersion(tableVersion);
		}

		public KVPair translate(KVPair mutation) throws IOException, StandardException {
				if (mutation == null) return null;

				EntryAccumulator keyAccumulator = getKeyAccumulator();
				keyAccumulator.reset();

				boolean hasNullKeyFields = false;
				/*
				 * Data comes in for transformation looking like:
				 *
				 * <key1>|<key2>|...<keyN> -> <row1>...<rowM>
				 *
				 * and we need to take some of those key columns and some of those row columns to form
				 * a new row key, which we will then emit (along with an empty [] for the data).
				 *
				 * First, we go through the main key columns,and if any of them are present in the keyEncodingMap,
				 * we encode. Otherwise, we skip over it (using the columnTypes field).
				 *
				 * Then, we iterate through the row columns and find any fields which are indexed and we
				 * index those as well.
				 *
				 * Finally, we add a row key onto the key, and return it.
				 */
				if(sourceKeyColumnEncodingOrder!=null){
						//we have key columns to check
						MultiFieldDecoder keyDecoder = getKeyDecoder();
						keyDecoder.set(mutation.getRow());
						for(int i=0;i<sourceKeyColumnEncodingOrder.length;i++){
								int sourceKeyColumnPos = sourceKeyColumnEncodingOrder[i];
								int destKeyPos = sourceKeyColumnPos<destKeyEncodingMap.length? destKeyEncodingMap[sourceKeyColumnPos]:-1;
								if(destKeyPos<0){
										skip(keyDecoder, columnTypes[i]);
								}else{
										int offset = keyDecoder.offset();
										boolean isNull = skip(keyDecoder, columnTypes[i]);
										hasNullKeyFields = isNull ||hasNullKeyFields;
										if(!isNull){
												int length = keyDecoder.offset()-offset-1;
												accumulate(keyAccumulator,destKeyPos,
																columnTypes[sourceKeyColumnPos],
																sourceKeyColumnSortOrder!=null && sourceKeyColumnSortOrder[i]!=destKeyColumnSortOrder[sourceKeyColumnPos],
																keyDecoder.array(),offset,length);
										}
								}
						}
				}

				//accumulate the row columns now
				EntryDecoder rowDecoder = getRowDecoder();
				rowDecoder.set(mutation.getValue());

				Indexed index = rowDecoder.getCurrentIndex();
				MultiFieldDecoder rowFieldDecoder = rowDecoder.getEntryDecoder();
				for(int i=index.nextSetBit(0);i>=0;i=index.nextSetBit(i+1)){
						int keyColumnPos = i<destKeyEncodingMap.length? destKeyEncodingMap[i]: -1;
						if(keyColumnPos<0){
								rowDecoder.seekForward(rowFieldDecoder,i);
						}else{
								int offset = rowFieldDecoder.offset();
								boolean isNull = rowDecoder.seekForward(rowFieldDecoder, i);
								hasNullKeyFields = isNull ||hasNullKeyFields;
								if(!isNull){
										int length = rowFieldDecoder.offset()-offset-1;
										accumulate(keyAccumulator,
														keyColumnPos,
														getType(index,i),
														!destKeyColumnSortOrder[keyColumnPos],
														rowFieldDecoder.array(),offset,length);
								}
						}
				}

				//add the row key to the end of the index key
				byte[] rowData = Encoding.encodeBytesUnsorted(mutation.getRow());

				byte[] keyData;
				EntryEncoder rowEncoder = getRowEncoder();
				MultiFieldEncoder entryEncoder = rowEncoder.getEntryEncoder();
				entryEncoder.reset();
				entryEncoder.setRawBytes(rowData);
				byte[] rowColumnData = rowEncoder.encode();

				if(isUnique){
						keyData = getIndexRowKey(rowData,isUniqueWithDuplicateNulls &&(hasNullKeyFields||!keyAccumulator.isFinished()));
				} else
						keyData = getIndexRowKey(rowData,true);

				return new KVPair(keyData,rowColumnData,mutation.getType());
		}

		public byte[] getIndexRowKey(byte[] rowLocation,boolean nonUnique){
				byte[] data = destKeyAccumulator.finish();
				if(nonUnique){
						//append the row location to the end of the bytes
						byte[] newData = Arrays.copyOf(data,data.length+rowLocation.length+1);
						System.arraycopy(rowLocation,0,newData,data.length+1,rowLocation.length);
						data = newData;
				}
				return data;
		}

		public EntryEncoder getRowEncoder() {
				if(destRowEncoder==null){
						BitSet nonNullFields = new BitSet();
						int highestSetPosition = 0;
						for(int keyColumn:destKeyEncodingMap){
								if(keyColumn>highestSetPosition)
										highestSetPosition = keyColumn;
						}
						nonNullFields.set(highestSetPosition+1);
						destRowEncoder = EntryEncoder.create(SpliceKryoRegistry.getInstance(),1,nonNullFields,
										BitSet.newInstance(),BitSet.newInstance(),BitSet.newInstance());
				}
				return destRowEncoder;
		}

		private int getType(Indexed index, int pos) {
				if(index.isScalarType(pos)) return StoredFormatIds.SQL_INTEGER_ID;
				else if(index.isDoubleType(pos)) return StoredFormatIds.SQL_DOUBLE_ID;
				else if(index.isFloatType(pos)) return StoredFormatIds.SQL_REAL_ID;
				else return StoredFormatIds.SQL_VARCHAR_ID; //something to be known to be untyped
		}

		private void accumulate(EntryAccumulator keyAccumulator, int pos,
														int type,
														boolean reverseOrder,
														byte[] array, int offset, int length) {
				byte[] data = array;
				int off = offset;
				if(reverseOrder){
						//TODO -sf- could we cache these byte[] somehow?
						data = new byte[length];
						System.arraycopy(array,offset,data,0,length);
						for(int i=0;i<data.length;i++){
								data[i]^=0xff;
						}
						off = 0;
				}
				if(typeProvider.isScalar(type))
						keyAccumulator.addScalar(pos,data,off,length);
				else if(typeProvider.isDouble(type))
						keyAccumulator.addDouble(pos,data,off,length);
				else if(typeProvider.isFloat(type))
						keyAccumulator.addFloat(pos,data,off,length);
				else
						keyAccumulator.add(pos,data,off,length);

		}

		private boolean skip(MultiFieldDecoder keyDecoder, int sourceKeyColumnType) {
				boolean isNull;
				if(typeProvider.isScalar(sourceKeyColumnType)){
						isNull = keyDecoder.nextIsNull();
						keyDecoder.skipLong();
				}else if(typeProvider.isDouble(sourceKeyColumnType)){
						isNull = keyDecoder.nextIsNullDouble();
						keyDecoder.skipDouble();
				}else if(typeProvider.isFloat(sourceKeyColumnType)){
						isNull = keyDecoder.nextIsNullFloat();
						keyDecoder.skipFloat();
				}else{
						isNull = keyDecoder.nextIsNull();
						keyDecoder.skip();
				}
				return isNull;
		}

		private MultiFieldDecoder getKeyDecoder() {
				if(keyDecoder==null)
						keyDecoder = MultiFieldDecoder.create();
				return keyDecoder;
		}

		public ByteEntryAccumulator getKeyAccumulator() {
				if(destKeyAccumulator==null){
						BitSet keyFields = BitSet.newInstance();
						for(int keyColumn:destKeyEncodingMap){
								if(keyColumn>=0)
										keyFields.set(keyColumn);
						}
						destKeyAccumulator = new ByteEntryAccumulator(EntryPredicateFilter.emptyPredicate(),keyFields);
				}

				return destKeyAccumulator;
		}

		public EntryDecoder getRowDecoder() {
				if(rowDecoder==null)
						rowDecoder = new EntryDecoder(SpliceKryoRegistry.getInstance());
				return rowDecoder;
		}

		public boolean isUnique() {
				return isUnique;
		}
}
