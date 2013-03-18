package com.splicemachine.serialization;

import java.io.IOException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class NullRemovingRowKey extends UTF8RowKey {

			@Override public Class<?> getSerializedClass() { return String.class; }

			@Override
			public int getSerializedLength(Object o) throws IOException {
				return super.getSerializedLength(toUTF8(o));
			}

			private Object toUTF8(Object o) {
				//if(o==null|| o instanceof byte[]) return o;
				String replacedString = ((String) o ).replaceAll("\u0000","");
//				if(replacedString.length()<=0)
//					return null;
				return Bytes.toBytes((String) o);
			}

			@Override
			public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
				super.serialize(toUTF8(o),w);
			}

			@Override
			public Object deserialize(ImmutableBytesWritable w) throws IOException {
				byte[] b = (byte[])super.deserialize(w);
				return b ==null? b :  Bytes.toString(b);
			}
		}
