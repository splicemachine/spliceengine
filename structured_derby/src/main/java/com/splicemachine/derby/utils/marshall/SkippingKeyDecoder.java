package com.splicemachine.derby.utils.marshall;

import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

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
		private final FormatableBitSet accessedKeys;
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
				this.accessedKeys = accessedKeys;
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
						fieldDecoder = MultiFieldDecoder.create(SpliceKryoRegistry.getInstance());

				fieldDecoder.set(bytes,offset,length);
				unpack(destination,fieldDecoder);

		}

		protected void unpack(ExecRow destination, MultiFieldDecoder fieldDecoder) throws StandardException {
				DataValueDescriptor[] fields = destination.getRowArray();
				for(int i=0;i< keyColumnEncodingOrder.length;i++){
						int keyColumnPosition = keyColumnEncodingOrder[i];
						if(keyColumnPosition<0||(accessedKeys!=null && !accessedKeys.get(keyColumnPosition))){
							skip(i,fieldDecoder);
						}else{
								DescriptorSerializer serializer = serializers[keyDecodingMap[keyColumnPosition]];
								DataValueDescriptor field = fields[keyDecodingMap[keyColumnPosition]];
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
