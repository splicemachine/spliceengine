package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
public interface DescriptorSerializer extends Closeable {
		void encode(MultiFieldEncoder fieldEncoder,DataValueDescriptor dvd, boolean desc) throws StandardException;

		byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException;

		void decode(MultiFieldDecoder fieldDecoder,DataValueDescriptor destDvd, boolean desc) throws StandardException;

		void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException;

		boolean isScalarType();

		boolean isFloatType();

		boolean isDoubleType();

		public static interface Factory<T extends DescriptorSerializer>{

				T newInstance();
				/**
				 * @param dvd the dvd to encode/decode
				 * @return true if this serializer can be used to encode/decode this DataValueDescriptor
				 */
				boolean applies(DataValueDescriptor dvd);

				/**
				 *
				 * @param typeFormatId the Derby type format id for the descriptor
				 * @return true if this serializer can be used to encode/decode DataValueDescriptors with
				 * this typeFormatId
				 */
				boolean applies(int typeFormatId);

				boolean isScalar();
				boolean isFloat();
				boolean isDouble();
		}
}
