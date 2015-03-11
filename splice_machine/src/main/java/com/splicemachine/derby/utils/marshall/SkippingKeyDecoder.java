package com.splicemachine.derby.utils.marshall;

import com.carrotsearch.hppc.BitSet;
import com.google.common.io.Closeables;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/9/14
 */
public class SkippingKeyDecoder implements KeyHashDecoder {
		private byte[] bytes;
		private int offset;
		private int length;
		private MultiFieldDecoder fieldDecoder;

		private final TypeProvider typeProvider;
		protected final DescriptorSerializer[] serializers;

		private final int[] keyColumnEncodingOrder;
		private final int[] keyColumnTypes;
		private final BitSet accessedColumns;
		private final int[] keyDecodingMap;

		public static SkippingKeyDecoder decoder(TypeProvider typeProvider,
																						 DescriptorSerializer[] serializers,
																						 int[] keyColumnEncodingOrder,
																						 int[] keyColumnTypes,
																						 boolean[] keyColumnSortOrder,
																						 int[] keyDecodingMap,
																						 FormatableBitSet accessedKeys){
				if(keyColumnSortOrder!=null)
						return new Ordered(serializers,typeProvider,keyColumnEncodingOrder,accessedKeys,keyColumnTypes,keyColumnSortOrder,keyDecodingMap);
				else
						return new SkippingKeyDecoder(serializers, typeProvider, keyColumnEncodingOrder, accessedKeys, keyColumnTypes,keyDecodingMap);

		}

		private SkippingKeyDecoder(DescriptorSerializer[] serializers,
															 TypeProvider typeProvider,
															int[] keyColumnEncodingOrder,
															FormatableBitSet accessedKeys,
															int[] keyColumnTypes,
															int[] keyDecodingMap) {
				this.serializers = serializers;
				this.keyColumnEncodingOrder = keyColumnEncodingOrder;
				if(accessedKeys !=null){
						this.accessedColumns = new BitSet(accessedKeys.getLength());
						for(int i= accessedKeys.anySetBit();i>=0;i= accessedKeys.anySetBit(i)){
								accessedColumns.set(i);
						}
				}else{
						this.accessedColumns = null;
				}
				this.keyColumnTypes = keyColumnTypes;
				this.typeProvider = typeProvider;
				this.keyDecodingMap = keyDecodingMap;
		}

		@Override
		public void set(byte[] bytes, int hashOffset, int length) {
				this.bytes = bytes;
				this.offset = hashOffset;
				this.length = length;

		}

		@Override
		public void decode(ExecRow destination) throws StandardException {
				if(fieldDecoder==null)
						fieldDecoder = MultiFieldDecoder.create();

				fieldDecoder.set(bytes,offset,length);
				unpack(destination,fieldDecoder);

		}

		protected void unpack(ExecRow destination, MultiFieldDecoder fieldDecoder) throws StandardException {
				DataValueDescriptor[] fields = destination.getRowArray();
				for(int i=0;i< keyColumnEncodingOrder.length;i++){
						int keyColumnPosition = keyColumnEncodingOrder[i];
						if(keyColumnPosition<0||(accessedColumns !=null && !accessedColumns.get(i))){
							skip(i,fieldDecoder);
						}else{
								DescriptorSerializer serializer = serializers[keyDecodingMap[i]];
								DataValueDescriptor field = fields[keyDecodingMap[i]];
								serializer.decode(fieldDecoder, field, getSortOrder(i));
						}
				}
		}

		protected boolean getSortOrder(int sortPosition) {
				return false;
		}

		private void skip(int keyColumnPosition, MultiFieldDecoder fieldDecoder) {
				int colType = keyColumnTypes[keyColumnPosition];
				if(typeProvider.isScalar(colType))
						fieldDecoder.skipLong();
				else if(typeProvider.isFloat(colType))
						fieldDecoder.skipFloat();
				else if(typeProvider.isDouble(colType))
						fieldDecoder.skipDouble();
				else
						fieldDecoder.skip();
		}

		@Override
		public void close() throws IOException {
				for(DescriptorSerializer serializer:serializers){
						Closeables.closeQuietly(serializer);
				}
		}

		private static class Ordered extends SkippingKeyDecoder{

				private final boolean[] keySortOrder;
				private Ordered(DescriptorSerializer[] serializers,
												TypeProvider serializerMap,
												int[] keyColumns,
												FormatableBitSet accessedKeys,
												int[] keyColumnTypes,
												boolean[] keySortOrder,
												int[] keyDecodingMap) {
						super(serializers, serializerMap,keyColumns, accessedKeys, keyColumnTypes,keyDecodingMap);
						this.keySortOrder = keySortOrder;
				}

				@Override
				protected boolean getSortOrder(int sortPosition) {
						return !keySortOrder[sortPosition];
				}
		}
}
