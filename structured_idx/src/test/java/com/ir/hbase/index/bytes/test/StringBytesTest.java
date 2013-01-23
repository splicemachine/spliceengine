package com.ir.hbase.index.bytes.test;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexColumn;
import com.ir.hbase.client.index.IndexColumn.Order;
import com.ir.hbase.client.structured.Column.Type;

public class StringBytesTest {
	
	@Test
	public void testStringAscendingOrder() {
		IndexColumn icol = new IndexColumn("StringColumn", Order.ASCENDING, Type.STRING);
		//with String object
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Gu"), Index.dataToBytes(icol, "Guangle")) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "gu"), Index.dataToBytes(icol, "guangle")) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Fan"), Index.dataToBytes(icol, "Frank")) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Jim"), Index.dataToBytes(icol, "John")) < 0);
		//with bytes
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Gu".getBytes()), Index.dataToBytes(icol, "Guangle".getBytes())) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "gu".getBytes()), Index.dataToBytes(icol, "guangle".getBytes())) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Fan".getBytes()), Index.dataToBytes(icol, "Frank".getBytes())) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Jim".getBytes()), Index.dataToBytes(icol, "John".getBytes())) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Tomtom".getBytes()), Index.dataToBytes(icol, "Tomtom".getBytes())) == 0);
	}
	
	@Test
	public void testStringDescendingOrder() {
		IndexColumn icol = new IndexColumn("StringColumn", Order.DESCENDING, Type.STRING);
		//with String object
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Gu"), Index.dataToBytes(icol, "Guangle")) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "gu"), Index.dataToBytes(icol, "guangle")) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Fan"), Index.dataToBytes(icol, "Frank")) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Jim"), Index.dataToBytes(icol, "John")) > 0);
		//with bytes
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Gu".getBytes()), Index.dataToBytes(icol, "Guangle".getBytes())) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "gu".getBytes()), Index.dataToBytes(icol, "guangle".getBytes())) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Fan".getBytes()), Index.dataToBytes(icol, "Frank".getBytes())) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Jim".getBytes()), Index.dataToBytes(icol, "John".getBytes())) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, "Tomtom".getBytes()), Index.dataToBytes(icol, "Tomtom".getBytes())) == 0);
	}
}
