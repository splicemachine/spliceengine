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

import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RefDataValue;
import com.splicemachine.db.iapi.types.RowLocation;

public class RefDescriptorSerializer implements DescriptorSerializer {
	
	// Similar to UnsortedBinaryDescriptorSerializer except our byte array
	// is within the RowLocation within the DVD, instead of directly
	// contained by the DVD.

	private static final DescriptorSerializer INSTANCE = new RefDescriptorSerializer();
	
	public static final Factory INSTANCE_FACTORY = new Factory() {
		@Override
		public DescriptorSerializer newInstance() {
			return INSTANCE;
		}

		@Override
		public boolean applies(DataValueDescriptor dvd) {
			return dvd != null && applies(dvd.getTypeFormatId());
		}

		@Override
		public boolean applies(int typeFormatId) {
			return typeFormatId == StoredFormatIds.SQL_REF_ID;
		}

		@Override public boolean isScalar() { return false; }
		@Override public boolean isFloat() { return false; }
		@Override public boolean isDouble() { return false; }
	};

	private RefDescriptorSerializer() { }

	@Override
	public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
		fieldEncoder.encodeNextUnsorted(((RowLocation)dvd.getObject()).getBytes());
	}

	@Override
	public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
		return Encoding.encodeBytesUnsorted(((RowLocation)dvd.getObject()).getBytes());
	}

	@Override
	public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
		byte[] bytes = fieldDecoder.decodeNextBytesUnsorted();
		((RefDataValue)destDvd).setValue(new HBaseRowLocation(bytes));
	}

	@Override
	public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
		// TODO: dvd.getObject() probably returns NULL so instantiate RowLocation like in 'decode' method
		((RowLocation)dvd.getObject()).setValue(Encoding.decodeBytesUnsortd(data,offset,length));
	}

	@Override public boolean isScalarType() { return false; }
	@Override public boolean isFloatType() { return false; }
	@Override public boolean isDoubleType() { return false; }

	@Override public void close() { }
}
