/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils.marshall.dvd;

import org.spark_project.guava.io.Closeables;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.si.constants.SIConstants;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
public class NullDescriptorSerializer implements DescriptorSerializer{

		private DescriptorSerializer delegate;
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
										@Override public boolean isScalarType() { return false; }
										@Override public boolean isFloatType() { return false; }
										@Override public boolean isDoubleType() { return true; }
								};
						}
						@Override public boolean applies(DataValueDescriptor dvd) { return delegate.applies(dvd); }
						@Override public boolean applies(int typeFormatId) { return delegate.applies(typeFormatId); }

						@Override public boolean isScalar() { return delegate.isScalar(); }
						@Override public boolean isFloat() { return delegate.isFloat(); }
						@Override public boolean isDouble() { return delegate.isDouble(); }
				};
		}

		public static <T extends DescriptorSerializer> Factory nullFactory(final Factory<T> delegate, final boolean sparse){
				return new Factory() {
						@Override public DescriptorSerializer newInstance() { return new NullDescriptorSerializer(delegate.newInstance(),sparse); }
						@Override public boolean applies(DataValueDescriptor dvd) { return delegate.applies(dvd); }
						@Override public boolean applies(int typeFormatId) { return delegate.applies(typeFormatId); }
						@Override public boolean isScalar() { return delegate.isScalar(); }
						@Override public boolean isFloat() { return delegate.isFloat(); }
						@Override public boolean isDouble() { return delegate.isDouble(); }
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
										@Override public boolean isScalarType() { return false; }
										@Override public boolean isFloatType() { return true; }
										@Override public boolean isDoubleType() { return false; }
								};
						}
						@Override public boolean applies(DataValueDescriptor dvd) { return delegate.applies(dvd); }
						@Override public boolean applies(int typeFormatId) { return delegate.applies(typeFormatId); }
						@Override public boolean isScalar() { return delegate.isScalar(); }
						@Override public boolean isFloat() { return delegate.isFloat(); }
						@Override public boolean isDouble() { return delegate.isDouble(); }
				};
		}

		@Override public boolean isScalarType() { return delegate.isScalarType(); }
		@Override public boolean isFloatType() { return delegate.isFloatType(); }
		@Override public boolean isDoubleType() { return delegate.isDoubleType(); }

		@Override public void close() throws IOException{
				Closeables.close(delegate,true);
		}

		@Override
		public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
				boolean isNullLocal;
				if (dvd == null)
					isNullLocal = true;
                else
                    isNullLocal = dvd.isNull();
				if(isNullLocal) {
						if (!sparse) encodeEmpty(fieldEncoder);
						return;
				}
//                if (dvd.getObject() instanceof UDTBase) {
//                    delegate = UDTDescriptorSerializer.INSTANCE;
//                }
				delegate.encode(fieldEncoder,dvd,desc);
		}

		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				if(dvd==null||dvd.isNull()){
						if (!sparse) return empty();
						else return SIConstants.EMPTY_BYTE_ARRAY;
				}
				return delegate.encodeDirect(dvd,desc);
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				if(nextIsNull(fieldDecoder)){
						destDvd.setToNull();
						if(!sparse)skip(fieldDecoder);
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
				return SIConstants.EMPTY_BYTE_ARRAY;
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

		protected void skip(MultiFieldDecoder fieldDecoder) {
				if(isScalarType())
						fieldDecoder.skipLong();
				else if(isDoubleType())
						fieldDecoder.skipDouble();
				else if(isFloatType())
						fieldDecoder.skipFloat();
				else
						fieldDecoder.skip();
		}
}

