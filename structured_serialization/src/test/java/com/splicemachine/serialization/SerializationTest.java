package com.splicemachine.serialization;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

public class SerializationTest {
	
	@Test
	public void textSerdeTest() throws IOException {
		serializationDeserializationTest(String.class, "John Herman Leach",new NullRemovingRowKey());
	}

	@Test
	public void textSerTest() throws IOException {
		serializationTest(String.class, "John Herman Leach",new NullRemovingRowKey());
	}

	
	@Test
	public void textByteArrayConversion() throws IOException {
		for (int i=0;i<1000000;i++) {
			byte[] longByteArray = Bytes.toBytes("John Herman Leach");
			String johnString = Bytes.toString(longByteArray);
		}		
	}

	
	@Test
	public void longSerdeTest() throws IOException {
		serializationDeserializationTest(Long.class, 100L,new LongRowKey());
	}

	@Test
	public void longWritableSerdeTest() throws IOException {
		serializationDeserializationTest(Long.class, new LongWritable(100L),new LongWritableRowKey());
	}


	@Test
	public void longByteArrayConversion() throws IOException {
		for (int i=0;i<1000000;i++) {
			byte[] longByteArray = Bytes.toBytes(100l);
			long johnLong = Bytes.toLong(longByteArray);
		}		
	
	}

	@Test
	public void longByteArrayConversionSortable() throws IOException {
		for (int i=0;i<1000000;i++) {
			byte[] longByteArray = SortableByteUtil.toBytes(100l);
			//long johnLong = Bytes.toLong(longByteArray);
		}		
	
	}

	
	
	private <T> void serializationDeserializationTest(Class<T> instance, Object value, RowKey rowKey) throws IOException {
		for (int i=0;i<1000000;i++) {
			byte[] longByteArray = rowKey.serialize(value);
			T returnValue = (T) rowKey.deserialize(longByteArray);
		}
	}

	private <T> void serializationTest(Class<T> instance, Object value, RowKey rowKey) throws IOException {
		for (int i=0;i<1000000;i++) {
			byte[] longByteArray = rowKey.serialize(value);
		}
	}

}
