package com.ir.hbase.index.bytes.test;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexColumn;
import com.ir.hbase.client.index.IndexColumn.Order;
import com.ir.hbase.client.structured.Column.Type;

public class FloatBytesTest {
	@Test
	public void testIntegerAscendingOrder() {
		IndexColumn icol = new IndexColumn("FloatColumn", Order.ASCENDING, Type.FLOAT);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, (float) -0.000001), Index.dataToBytes(icol, (float) -0.000002)) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, (float) 100001), Index.dataToBytes(icol, (float) 100000)) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, (float) 4.3333333), Index.dataToBytes(icol, (float) 4.333332)) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, (float) 10000), Index.dataToBytes(icol, (float) 20)) > 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, (float) 2001), Index.dataToBytes(icol, (float) 2001)) == 0);
	}
	@Test
	public void testIntegerDescendingOrder() {
		IndexColumn icol = new IndexColumn("FloatColumn", Order.DESCENDING, Type.FLOAT);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, (float) -0.000001), Index.dataToBytes(icol, (float) -0.000002)) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, (float) 100001), Index.dataToBytes(icol, (float) 100000)) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, (float) 4.3333333), Index.dataToBytes(icol, (float) 4.333332)) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, (float) 10000), Index.dataToBytes(icol, (float) 20)) < 0);
		Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, (float) 2001), Index.dataToBytes(icol, (float) 2001)) == 0);
	}
}
