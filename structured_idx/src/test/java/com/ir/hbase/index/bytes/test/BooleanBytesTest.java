package com.ir.hbase.index.bytes.test;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexColumn;
import com.ir.hbase.client.index.IndexColumn.Order;
import com.ir.hbase.client.structured.Column.Type;

public class BooleanBytesTest {
	@Test
	public void testIntegerAscendingOrder() {
		IndexColumn icol = new IndexColumn("BooleanColumn", Order.ASCENDING, Type.BOOLEAN);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, true), Index.dataToBytes(icol, false)) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, true), Index.dataToBytes(icol, false)) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, true), Index.dataToBytes(icol, true)) == 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, false), Index.dataToBytes(icol, false)) == 0);
	}
	@Test
	public void testIntegerDescendingOrder() {
		IndexColumn icol = new IndexColumn("BooleanColumn", Order.DESCENDING, Type.BOOLEAN);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, true), Index.dataToBytes(icol, false)) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, true), Index.dataToBytes(icol, false)) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, true), Index.dataToBytes(icol, true)) == 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, false), Index.dataToBytes(icol, false)) == 0);
	}
	
}