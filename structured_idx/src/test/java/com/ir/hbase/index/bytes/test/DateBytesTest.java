package com.ir.hbase.index.bytes.test;

import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexColumn;
import com.ir.hbase.client.index.IndexColumn.Order;
import com.ir.hbase.client.structured.Column.Type;

public class DateBytesTest {
	@Test
	public void testDateAscendingOrder() {
		IndexColumn icol = new IndexColumn("DateColumn", Order.ASCENDING, Type.DATE);
		Date date1 = new Date();
		Date date2 = new Date();
		Date date3 = new Date();
		for (long l = 10; l < 100; ++l) {
			Date date = new Date();
			if (l % 2 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, date), Index.dataToBytes(icol, date1)) >= 0);
				date1 = date;
			} else if (l % 3 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, date), Index.dataToBytes(icol, date2)) >= 0);
				date2 = date;
			} else if (l % 5 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, date), Index.dataToBytes(icol, date3)) >= 0);
				date3 = date;
			}
		}
	}
	@Test
	public void testDateDescendingOrder() {
		IndexColumn icol = new IndexColumn("DateColumn", Order.DESCENDING, Type.DATE);
		Date date1 = new Date();
		Date date2 = new Date();
		Date date3 = new Date();
		for (long l = 10; l < 100; ++l) {
			Date date = new Date();
			if (l % 2 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, date), Index.dataToBytes(icol, date1)) <= 0);
				date1 = date;
			} else if (l % 3 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, date), Index.dataToBytes(icol, date2)) <= 0);
				date2 = date;
			} else if (l % 5 == 0) {
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, date), Index.dataToBytes(icol, date3)) <= 0);
				date3 = date;
			}
		}
	}
}
