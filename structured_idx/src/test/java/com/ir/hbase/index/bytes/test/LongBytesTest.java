package com.ir.hbase.index.bytes.test;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexColumn;
import com.ir.hbase.client.index.IndexColumn.Order;
import com.ir.hbase.client.structured.Column.Type;

public class LongBytesTest {
	@Test
	public void testLongAscendingOrder() {
		IndexColumn icol = new IndexColumn("LongColumn", Order.ASCENDING, Type.LONG);
		//with String object
		long l1 = Long.MIN_VALUE, l2 = Long.MIN_VALUE, l3 = Long.MIN_VALUE;
		for (long l = -1000; l < 1000; ++l) {
			if (l % 2 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, l1), Index.dataToBytes(icol, l)) < 0);
				l1 = l;
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, l1), Index.dataToBytes(icol, l)) == 0);
			} else if (l % 3 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, l2), Index.dataToBytes(icol, l)) < 0);
				l2 = l;
			} else if (l % 5 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, l3), Index.dataToBytes(icol, l)) < 0);
				l3 = l;
			}
		}
	}
	@Test
	public void testLongDescendingOrder() {
		IndexColumn icol = new IndexColumn("LongColumn", Order.DESCENDING, Type.LONG);
		long l1 = Long.MIN_VALUE, l2 = Long.MIN_VALUE, l3 = Long.MIN_VALUE;
		for (long l = -1000; l < 1000; ++l) {
			if (l % 2 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, l1), Index.dataToBytes(icol, l)) > 0);
				l1 = l;
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, l1), Index.dataToBytes(icol, l)) == 0);
			} else if (l % 3 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, l2), Index.dataToBytes(icol, l)) > 0);
				l2 = l;
			} else if (l % 5 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, l3), Index.dataToBytes(icol, l)) > 0);
				l3 = l;
			}
		}
	}
}
