package com.splicemachine.derby.utils.marshall.dvd;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/2/14
 */
public class KryoDescriptorSerializer implements DescriptorSerializer,Closeable {
		private final KryoPool kryoPool;

		private Kryo kryo;
		private Output output;
		private Input input;

		public KryoDescriptorSerializer(KryoPool kryoPool) {
				this.kryoPool = kryoPool;
		}

		public static Factory newFactory(final KryoPool kryoPool){
				return new Factory() {
						@Override public DescriptorSerializer newInstance() { return new KryoDescriptorSerializer(kryoPool); }

						@Override public boolean applies(DataValueDescriptor dvd) { return dvd!=null && applies(dvd.getTypeFormatId()); }

						@Override
						public boolean applies(int typeFormatId) {
								switch(typeFormatId){
										case StoredFormatIds.SQL_REF_ID:
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

				kryo.writeClassAndObject(output,o);
				fieldEncoder.encodeNextUnsorted(output.toBytes());
		}


		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				initializeForWrite();
				Object o = dvd.getObject();
				kryo.writeClassAndObject(output,o);
				return Encoding.encodeBytesUnsorted(output.toBytes());
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				initializeForReads();
				input.setBuffer(fieldDecoder.decodeNextBytesUnsorted());
				Object o = kryo.readClassAndObject(input);
				destDvd.setValue(o);
		}


		@Override
		public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
				initializeForReads();
				input.setBuffer(Encoding.decodeBytesUnsortd(data,offset,length));
				Object o = kryo.readClassAndObject(input);
				dvd.setValue(o);
		}

		@Override public boolean isScalarType() { return false; }
		@Override public boolean isFloatType() { return false; }
		@Override public boolean isDoubleType() { return false; }

		@Override
		public void close() throws IOException {
				if(kryo!=null)
						kryoPool.returnInstance(kryo);
		}

		private void initializeForReads() {
				initializeKryo();
				if(input==null)
						input = new Input();
		}

		private void initializeForWrite() {
				initializeKryo();
				if(output==null)
						output = new Output(20,-1);
				else
						output.clear();
		}

		private void initializeKryo() {
				if(kryo==null)
						kryo = kryoPool.get();
		}
}
