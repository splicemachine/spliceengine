package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.derby.impl.sql.execute.LazyDataValueDescriptor;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
public class LazyDescriptorSerializer implements DescriptorSerializer {
		protected final DescriptorSerializer delegate;
		protected final String tableVersion;

		public LazyDescriptorSerializer(DescriptorSerializer delegate, String tableVersion) {
				this.delegate = delegate;
				this.tableVersion = tableVersion;
		}


		public static Factory factory(final Factory delegateFactory, final String tableVersion){
				return new Factory() {
                        @Override public DescriptorSerializer newInstance() {
                            return new LazyDescriptorSerializer(delegateFactory.newInstance(),
                                                                    tableVersion);
                        }

						@Override public boolean applies(DataValueDescriptor dvd) { return delegateFactory.applies(dvd); }
						@Override public boolean applies(int typeFormatId) { return delegateFactory.applies(typeFormatId); }

						@Override public boolean isScalar() { return delegateFactory.isScalar(); }
						@Override public boolean isFloat() { return delegateFactory.isFloat(); }
						@Override public boolean isDouble() { return delegateFactory.isDouble(); }
				};
		}

		@Override
		public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
				if(dvd.isLazy()){
						LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor)dvd;

						ldvd.encodeInto(fieldEncoder,desc,tableVersion);
				}else
						delegate.encode(fieldEncoder,dvd,desc);
		}

		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				if(dvd.isLazy()){
						LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor) dvd;
						ldvd.setSerializer(VersionedSerializers.forVersion(tableVersion,true).getEagerSerializer(dvd.getTypeFormatId()));
						return ldvd.getBytes(desc, tableVersion);
				}
				else return delegate.encodeDirect(dvd,desc);
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				if(destDvd.isLazy()){
						LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor)destDvd;
						int offset = fieldDecoder.offset();
						DerbyBytesUtil.skipField(fieldDecoder, destDvd);
						int length = fieldDecoder.offset()-offset-1;
						ldvd.initForDeserialization(tableVersion,delegate,fieldDecoder.array(),offset,length,desc);
				}else
						delegate.decode(fieldDecoder,destDvd,desc);
		}

		@Override
		public void decodeDirect(DataValueDescriptor destDvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
				if(destDvd.isLazy()){
						LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor)destDvd;
						ldvd.initForDeserialization(tableVersion,delegate, data, offset, length, desc);
				}else
						delegate.decodeDirect(destDvd,data,offset,length,desc);
		}

		@Override public boolean isScalarType() { return delegate.isScalarType(); }
		@Override public boolean isFloatType() { return delegate.isFloatType(); }
		@Override public boolean isDoubleType() { return delegate.isDoubleType(); }

		@Override public void close() throws IOException { delegate.close(); }
}
