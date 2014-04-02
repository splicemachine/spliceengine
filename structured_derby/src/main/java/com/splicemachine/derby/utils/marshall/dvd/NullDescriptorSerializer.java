package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.HConstants;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
public class NullDescriptorSerializer implements DescriptorSerializer{
		private final DescriptorSerializer delegate;
		private final boolean sparse;

		public NullDescriptorSerializer(DescriptorSerializer delegate,boolean sparse) {
				this.delegate = delegate;
				this.sparse = sparse;
		}

		public static <T extends DescriptorSerializer> Factory doubleChecker(final Factory<T> delegate, final boolean sparse){
				return new Factory() {
						@Override
						public DescriptorSerializer newInstance() {
								return new NullDescriptorSerializer(delegate.newInstance(),sparse){

										@Override protected void encodeEmpty(MultiFieldEncoder fieldEncoder) { fieldEncoder.encodeEmptyDouble(); }
										@Override protected byte[] empty() { return Encoding.encodedNullDouble(); }
										@Override protected boolean nextIsNull(MultiFieldDecoder fieldDecoder) { return fieldDecoder.nextIsNullDouble(); }
										@Override protected boolean isNull(byte[] data, int offset, int length) { return Encoding.isNullDOuble(data,offset,length); }
								};
						}
						@Override public boolean applies(DataValueDescriptor dvd) { return delegate.applies(dvd); }
						@Override public boolean applies(int typeFormatId) { return delegate.applies(typeFormatId); }
				};
		}

		public static <T extends DescriptorSerializer> Factory nullFactory(final Factory<T> delegate, final boolean sparse){
				return new Factory() {
						@Override public DescriptorSerializer newInstance() { return new NullDescriptorSerializer(delegate.newInstance(),sparse); }
						@Override public boolean applies(DataValueDescriptor dvd) { return delegate.applies(dvd); }
						@Override public boolean applies(int typeFormatId) { return delegate.applies(typeFormatId); }
				};
		}

		public static <T extends DescriptorSerializer> Factory floatChecker(final Factory<T> delegate, final boolean sparse){
				return new Factory() {
						@Override
						public DescriptorSerializer newInstance() {
								return new NullDescriptorSerializer(delegate.newInstance(),sparse){
										@Override protected void encodeEmpty(MultiFieldEncoder fieldEncoder) { fieldEncoder.encodeEmptyFloat(); }
										@Override protected byte[] empty() { return Encoding.encodedNullFloat(); }
										@Override protected boolean nextIsNull(MultiFieldDecoder fieldDecoder) { return fieldDecoder.nextIsNullFloat(); }
										@Override protected boolean isNull(byte[] data, int offset, int length) { return Encoding.isNullFloat(data, offset, length); }
								};
						}
						@Override public boolean applies(DataValueDescriptor dvd) { return delegate.applies(dvd); }
						@Override public boolean applies(int typeFormatId) { return delegate.applies(typeFormatId); }
				};
		}

		@Override
		public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
				if(dvd==null||dvd.isNull()){
						if (!sparse) encodeEmpty(fieldEncoder);
						return;
				}
				delegate.encode(fieldEncoder,dvd,desc);
		}

		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				if(dvd==null||dvd.isNull()){
						if (!sparse) return empty();
						else return HConstants.EMPTY_BYTE_ARRAY;
				}
				return delegate.encodeDirect(dvd,desc);
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				if(nextIsNull(fieldDecoder)){
						destDvd.setToNull();
						return;
				}
				delegate.decode(fieldDecoder,destDvd,desc);
		}


		@Override
		public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
				if(isNull(data,offset,length)){
						dvd.setToNull();
						return;
				}
				delegate.decodeDirect(dvd, data, offset, length, desc);
		}


		/**
		 * Override to provide different encodings of {@code null}
		 * @param fieldEncoder the encoder to use
		 */
		protected void encodeEmpty(MultiFieldEncoder fieldEncoder) {
				fieldEncoder.encodeEmpty();
		}

		/**
		 * Override to provide different null entries
		 * @return a representation of {@code null} for this type.
		 */
		protected byte[] empty() {
				return HConstants.EMPTY_BYTE_ARRAY;
		}

		/**
		 * Override to check for different kinds of null
		 * @param fieldDecoder the field decoder to use
		 * @return true if the field is null.
		 */
		protected boolean nextIsNull(MultiFieldDecoder fieldDecoder) {
				return fieldDecoder.nextIsNull();
		}

		/**
		 * Override to check for different kinds of null
		 *
		 * @param data the data to check
		 * @param offset the offset of the field
		 * @param length the length of the field
		 * @return true if the next field is null.
		 */
		protected boolean isNull(byte[] data, int offset, int length) {
				return length<=0;
		}
}

