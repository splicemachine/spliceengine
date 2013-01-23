package com.ir.hbase.index.bytes.test;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexColumn;
import com.ir.hbase.client.index.IndexColumn.Order;
import com.ir.hbase.client.structured.Column.Type;

public class IntegerBytesTest {
	@Test
	public void testIntegerAscendingOrder() {
		IndexColumn icol = new IndexColumn("IntegerColumn", Order.ASCENDING, Type.INTEGER);
		int i1 = Integer.MIN_VALUE, i2 = Integer.MIN_VALUE, i3 = Integer.MIN_VALUE;
		for (int i = -1000; i < 1000; ++i) {
			if (i % 2 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, i1), Index.dataToBytes(icol, i)) < 0);
				i1 = i;
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, i1), Index.dataToBytes(icol, i)) == 0);
			} else if (i % 3 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, i2), Index.dataToBytes(icol, i)) < 0);
				i2 = i;
			} else if (i % 5 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, i3), Index.dataToBytes(icol, i)) < 0);
				i3 = i;
			}
		}
	}
	@Test
	public void testIntegerDescendingOrder() {
		IndexColumn icol = new IndexColumn("IntegerColumn", Order.DESCENDING, Type.INTEGER);
		int i1 = Integer.MIN_VALUE, i2 = Integer.MIN_VALUE, i3 = Integer.MIN_VALUE;
		for (int i = -1000; i < 1000; ++i) {
			if (i % 2 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, i1), Index.dataToBytes(icol, i)) > 0);
				i1 = i;
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, i1), Index.dataToBytes(icol, i)) == 0);
			} else if (i % 3 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, i2), Index.dataToBytes(icol, i)) > 0);
				i2 = i;
			} else if (i % 5 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, i3), Index.dataToBytes(icol, i)) > 0);
				i3 = i;
			}
		}
	}
}
