/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import splice.com.google.common.base.Throwables;
import com.splicemachine.db.shared.common.udt.UDTBase;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/2/14
 */
public class KryoDescriptorSerializer implements DescriptorSerializer,Closeable {
		private final KryoPool kryoPool;

		private Output output;
		private Input input;

		public KryoDescriptorSerializer(KryoPool kryoPool) {
				this.kryoPool = kryoPool;
		}

		public static Factory newFactory(final KryoPool kryoPool){
				return new Factory() {
						@Override public DescriptorSerializer newInstance() { return new KryoDescriptorSerializer(kryoPool); }

						@Override public boolean applies(DataValueDescriptor dvd) {
                            if (dvd == null)
                                return false;

                            try {
                                Object o = dvd.getObject();
                                if (o!= null && o instanceof UDTBase)
                                    return false;
                                else
                                    return applies(dvd.getTypeFormatId());
                            } catch (Exception e) {
                                throw new RuntimeException(Throwables.getRootCause(e));
                            }
                        }

						@Override
						public boolean applies(int typeFormatId) {
								switch(typeFormatId){
										// Starting with Fuji release, we handle SQL_REF serialization
										// with RefDescriptorSerilizer class.
										// case StoredFormatIds.SQL_REF_ID:
										case StoredFormatIds.SQL_USERTYPE_ID_V3:
												return true;
										default:
												return false;
								}
						}

						@Override public boolean isScalar() { return false; }
						@Override public boolean isFloat() { return false; }
						@Override public boolean isDouble() { return false; }
				};
		}


		@Override
		public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
				initializeForWrite();
				Object o = dvd.getObject();
                Kryo kryo = kryoPool.get();
                try {
                    kryo.writeClassAndObject(output, o);
                    fieldEncoder.encodeNextUnsorted(output.toBytes());
                } finally {
                    kryoPool.returnInstance(kryo);
                }
		}


		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				initializeForWrite();
				Object o = dvd.getObject();
                Kryo kryo = kryoPool.get();
                try {
                    kryo.writeClassAndObject(output, o);
                    return Encoding.encodeBytesUnsorted(output.toBytes());
                } finally {
                    kryoPool.returnInstance(kryo);
                }
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				initializeForReads();
				input.setBuffer(fieldDecoder.decodeNextBytesUnsorted());
                Kryo kryo = kryoPool.get();
                try {
                    Object o = kryo.readClassAndObject(input);
                    destDvd.setValue(o);
                } finally {
                    kryoPool.returnInstance(kryo);
                }
		}


		@Override
		public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
				initializeForReads();
				input.setBuffer(Encoding.decodeBytesUnsortd(data,offset,length));
                Kryo kryo = kryoPool.get();
                try {
                    Object o = kryo.readClassAndObject(input);
                    dvd.setValue(o);
                } finally {
                    kryoPool.returnInstance(kryo);
                }
		}

		@Override public boolean isScalarType() { return false; }
		@Override public boolean isFloatType() { return false; }
		@Override public boolean isDoubleType() { return false; }

		@Override
		public void close() throws IOException {
		}

		private void initializeForReads() {
				if(input==null)
						input = new Input();
		}

		private void initializeForWrite() {
				if(output==null)
						output = new Output(20,-1);
				else
						output.clear();
		}

}
