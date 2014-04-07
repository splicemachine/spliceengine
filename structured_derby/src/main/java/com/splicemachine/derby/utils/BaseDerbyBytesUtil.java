package com.splicemachine.derby.utils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.EnumMap;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactoryImpl.Format;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;

public class BaseDerbyBytesUtil {
    protected interface Serializer{
        byte[] encode(DataValueDescriptor dvd) throws StandardException;

        void decode(byte[] data,DataValueDescriptor dvd) throws StandardException;

        void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException;

				void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc,Calendar calendar) throws StandardException;

        void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException;

        boolean isScalarType();

				void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException;
		}

		protected static abstract class AbstractSerializer implements Serializer{
				@Override
				public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc, Calendar calendar) throws StandardException {
						encodeInto(dvd,encoder,desc);
				}
		}

    protected static EnumMap<Format,Serializer> serializationMap = new EnumMap<Format, Serializer>(Format.class);
    static{
        serializationMap.put(Format.BOOLEAN, new AbstractSerializer() {
            @Override public byte[] encode(DataValueDescriptor dvd) throws StandardException { return Encoding.encode(dvd.getBoolean()); }
            @Override public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException { dvd.setValue(Encoding.decodeBoolean(data)); }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getBoolean(),desc);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextBoolean(desc));
            }

            @Override public boolean isScalarType() { return false;   }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
								dvd.setValue(Encoding.decodeBoolean(data,offset,false));
						}
				} );

        serializationMap.put(Format.DOUBLE, new AbstractSerializer() {
            @Override public byte[] encode(DataValueDescriptor dvd) throws StandardException { return Encoding.encode(dvd.getDouble()); }
            @Override public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException { dvd.setValue(Encoding.decodeDouble(data)); }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getDouble(),desc);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextDouble(desc));
            }

            @Override public boolean isScalarType() { return false;   }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
								dvd.setValue(Encoding.decodeDouble(data,offset,false));
						}
				});

        serializationMap.put(Format.TINYINT,new AbstractSerializer() {
            @Override public byte[] encode(DataValueDescriptor dvd) throws StandardException { return Encoding.encode(dvd.getByte()); }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(Encoding.decodeByte(data));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getByte(),desc);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextByte(desc));
            }

            @Override public boolean isScalarType() { return true;   }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
							dvd.setValue(Encoding.decodeByte(data,offset,false));
						}
				});

        serializationMap.put(Format.SMALLINT,new AbstractSerializer() {
            @Override public byte[] encode(DataValueDescriptor dvd) throws StandardException { return Encoding.encode(dvd.getShort()); }
            @Override public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException { dvd.setValue(Encoding.decodeShort(data)); }
            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getShort(),desc);
            }
            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextShort(desc));
            }

            @Override public boolean isScalarType() { return true;   }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
								dvd.setValue(Encoding.decodeShort(data,offset,false));
						}
				});

        serializationMap.put(Format.INTEGER,new AbstractSerializer() {
            @Override public byte[] encode(DataValueDescriptor dvd) throws StandardException { return Encoding.encode(dvd.getInt()); }
            @Override public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException { dvd.setValue(Encoding.decodeInt(data)); }
            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getInt(),desc);
            }
            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextInt(desc));
            }

            @Override public boolean isScalarType() { return true; }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
								dvd.setValue(Encoding.decodeInt(data,offset,false));
						}
				});

        serializationMap.put(Format.LONGINT,new AbstractSerializer() {
            @Override public byte[] encode(DataValueDescriptor dvd) throws StandardException { return Encoding.encode(dvd.getLong()); }
            @Override public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException { dvd.setValue(Encoding.decodeLong(data)); }
            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getLong(),desc);
            }
            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextLong(desc));
            }

            @Override public boolean isScalarType() { return true; }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
							dvd.setValue(Encoding.decodeLong(data,offset,false));
						}
				});

        serializationMap.put(Format.REAL,new AbstractSerializer() {
            @Override public byte[] encode(DataValueDescriptor dvd) throws StandardException { return Encoding.encode(dvd.getFloat()); }
            @Override public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException { dvd.setValue(Encoding.decodeFloat(data)); }
            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getFloat(),desc);
            }
            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextFloat(desc));
            }

            @Override public boolean isScalarType() { return false;   }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
							dvd.setValue(Encoding.decodeFloat(data,offset,false));
						}
				});
         serializationMap.put(Format.DECIMAL,new AbstractSerializer() {
            @Override public byte[] encode(DataValueDescriptor dvd) throws StandardException { return Encoding.encode((BigDecimal)dvd.getObject()); }
            @Override public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException { dvd.setBigDecimal(Encoding.decodeBigDecimal(data)); }
            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext((BigDecimal)dvd.getObject(),desc);
            }
            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setBigDecimal(decoder.decodeNextBigDecimal(desc));
            }

             @Override public boolean isScalarType() { return false; }

						 @Override
						 public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
								 dvd.setValue(Encoding.decodeBigDecimal(data,offset,length,false));
						 }
				 });
        Serializer refSerializer = new AbstractSerializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                Object o = dvd.getObject();
                Output output = new Output(20,-1);
                SpliceKryoRegistry.getInstance().get().writeClassAndObject(output,o);
                return Encoding.encodeBytesUnsorted(output.toBytes());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
								decode(dvd,data,0,data.length);
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNextObject(dvd.getObject());
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextObject());
            }

            @Override public boolean isScalarType() { return false; }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
								byte[] decoded = Encoding.decodeBytesUnsortd(data,offset,length);
								Input input = new Input(decoded);
								dvd.setValue(SpliceKryoRegistry.getInstance().get().readClassAndObject(input));
						}
				};
        serializationMap.put(Format.REF,refSerializer);
        serializationMap.put(Format.USERTYPE,refSerializer);

        serializationMap.put(Format.DATE,new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getDate(null).getTime());
            }

						@Override
						public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc, Calendar calendar) throws StandardException {
								encoder.encodeNext(dvd.getDate(calendar).getTime(),desc);
						}

						@Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(new Date(Encoding.decodeLong(data)));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
								encodeInto(dvd, encoder, desc, null);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(new Date(decoder.decodeNextLong(desc)));
            }

            @Override public boolean isScalarType() { return true;   }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
								dvd.setValue(new Date(Encoding.decodeLong(data,offset,false)));
						}
				});

        serializationMap.put(Format.TIME,new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getTime(null).getTime());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(new Time(Encoding.decodeLong(data)));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
								encodeInto(dvd,encoder,desc,null);
            }

						@Override
						public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc,Calendar calendar) throws StandardException {
								encoder.encodeNext(dvd.getTime(calendar).getTime(),desc);
						}

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(new Time(decoder.decodeNextLong(desc)));
            }

            @Override public boolean isScalarType() { return true; }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
								dvd.setValue(new Time(Encoding.decodeLong(data,offset,false)));
						}
				});
        serializationMap.put(Format.TIMESTAMP,new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getTimestamp(null).getTime());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(new Timestamp(Encoding.decodeLong(data)));
            }

						@Override
						public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc, Calendar calendar) throws StandardException {
								encoder.encodeNext(dvd.getTimestamp(calendar).getTime(),desc);
						}

						@Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
								encodeInto(dvd, encoder, desc, null);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(new Timestamp(decoder.decodeNextLong(desc)));
            }

            @Override public boolean isScalarType() { return true; }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
								dvd.setValue(new Timestamp(Encoding.decodeLong(data,offset,false)));
						}
				});

        Serializer stringSerializer = new AbstractSerializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getString());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(Encoding.decodeString(data));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getString(),desc);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextString(desc));
            }

            @Override public boolean isScalarType() { return false; }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
								dvd.setValue(Encoding.decodeString(data,offset,length,false));
						}
				};
        serializationMap.put(Format.VARCHAR,stringSerializer);
        serializationMap.put(Format.LONGVARCHAR,stringSerializer);
        serializationMap.put(Format.CLOB,stringSerializer);
        serializationMap.put(Format.XML,stringSerializer);
        serializationMap.put(Format.CHAR,stringSerializer);

        Serializer byteSerializer = new AbstractSerializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encodeBytesUnsorted(dvd.getBytes());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(Encoding.decodeBytesUnsortd(data,0,data.length));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNextUnsorted(dvd.getBytes());
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextBytesUnsorted());
            }

            @Override public boolean isScalarType() { return false; }

						@Override
						public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
								dvd.setValue(Encoding.decodeBytesUnsortd(data,offset,length));
						}
				};
        serializationMap.put(Format.VARBIT,byteSerializer);
        serializationMap.put(Format.LONGVARBIT,byteSerializer);
        serializationMap.put(Format.BLOB,byteSerializer); //TODO -sf- this isn't going to be right for long
        serializationMap.put(Format.BIT,byteSerializer);
        serializationMap.put(Format.ROW_LOCATION,byteSerializer);
    }
}
