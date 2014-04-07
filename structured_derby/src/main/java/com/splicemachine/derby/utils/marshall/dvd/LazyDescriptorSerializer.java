package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.derby.impl.sql.execute.LazyDataValueDescriptor;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
public class LazyDescriptorSerializer implements DescriptorSerializer {
		protected final DescriptorSerializer delegate;

		public LazyDescriptorSerializer(DescriptorSerializer delegate) {
				this.delegate = delegate;
		}


		public static Factory singletonFactory(final Factory delegateFactory){
				DescriptorSerializer delegate = delegateFactory.newInstance();
				final LazyDescriptorSerializer me = new LazyDescriptorSerializer(delegate);
				return new Factory() {
						@Override
						public DescriptorSerializer newInstance() {
								return me;
						}

						@Override public boolean applies(DataValueDescriptor dvd) { return delegateFactory.applies(dvd); }
						@Override public boolean applies(int typeFormatId) { return delegateFactory.applies(typeFormatId); }
				};
		}

		@Override
		public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
				if(dvd.isLazy()){
						LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor)dvd;
						ldvd.encodeInto(fieldEncoder,desc);
				}else
						delegate.encode(fieldEncoder,dvd,desc);
		}

		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				if(dvd.isLazy())
						return dvd.getBytes();
				else return delegate.encodeDirect(dvd,desc);
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				if(destDvd.isLazy()){
						LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor)destDvd;
						int offset = fieldDecoder.offset();
						DerbyBytesUtil.skipField(fieldDecoder, destDvd);
						int length = fieldDecoder.offset()-offset-1;
						ldvd.initForDeserialization(fieldDecoder.array(),offset,length,desc);
				}else
						delegate.decode(fieldDecoder,destDvd,desc);
		}

		@Override
		public void decodeDirect(DataValueDescriptor destDvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
				if(destDvd.isLazy()){
						LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor)destDvd;
						ldvd.initForDeserialization(data,offset,length,desc);
				}else
						delegate.decodeDirect(destDvd,data,offset,length,desc);
		}

		@Override public boolean isScalarType() { return delegate.isScalarType(); }
		@Override public boolean isFloatType() { return delegate.isFloatType(); }
		@Override public boolean isDoubleType() { return delegate.isDoubleType(); }
}
