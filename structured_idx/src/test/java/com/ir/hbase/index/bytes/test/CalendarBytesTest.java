package com.ir.hbase.index.bytes.test;

import java.util.Calendar;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexColumn;
import com.ir.hbase.client.index.IndexColumn.Order;
import com.ir.hbase.client.structured.Column.Type;

public class CalendarBytesTest {
	@Test
	public void testCalendarAscendingOrder() {
		IndexColumn icol = new IndexColumn("CalendarColumn", Order.ASCENDING, Type.CALENDAR);
		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
		Calendar cal3 = Calendar.getInstance();
		Calendar cal = Calendar.getInstance();
		int year = 1980, month = 1, date = 1, hour = 12, minute = 1, second = 1;
		cal1.set(year, month, date, hour, minute, second);
		cal2.set(year, month, date, hour, minute, second);
		cal3.set(year, month, date, hour, minute, second);
		cal.set(year, month, date, hour, minute, second);

		for (long l = 1; l < 1000; ++l) {
			if (l % 2 == 0) {
				cal.add(Calendar.DATE, 1);
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, cal), Index.dataToBytes(icol, cal1)) > 0);
				cal1 = (Calendar) cal.clone();
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, cal), Index.dataToBytes(icol, cal1)) == 0);
			} else if (l % 3 == 0) {
				cal.add(Calendar.MONTH, 1);
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, cal), Index.dataToBytes(icol, cal2)) > 0);
				cal2 = (Calendar) cal.clone();
			} else if (l % 5 == 0) {
				cal.add(Calendar.YEAR, 1);
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, cal), Index.dataToBytes(icol, cal3)) > 0);
				cal3 = (Calendar) cal.clone();
			}
			cal.add(Calendar.HOUR, 1);
		}
	}
	
	@Test
	public void testCalendarDescendingOrder() {
		IndexColumn icol = new IndexColumn("CalendarColumn", Order.DESCENDING, Type.CALENDAR);
		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
		Calendar cal3 = Calendar.getInstance();
		Calendar cal = Calendar.getInstance();
		int year = 1980, month = 1, date = 1, hour = 12, minute = 1, second = 1;
		cal1.set(year, month, date, hour, minute, second);
		cal2.set(year, month, date, hour, minute, second);
		cal3.set(year, month, date, hour, minute, second);
		cal.set(year, month, date, hour, minute, second);

		for (long l = 1; l < 1000; ++l) {
			if (l % 2 == 0) {
				cal.add(Calendar.DATE, 1);
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, cal), Index.dataToBytes(icol, cal1)) < 0);
				cal1 = (Calendar) cal.clone();
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, cal), Index.dataToBytes(icol, cal1)) == 0);
			} else if (l % 3 == 0) {
				cal.add(Calendar.MONTH, 1);
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, cal), Index.dataToBytes(icol, cal2)) < 0);
				cal2 = (Calendar) cal.clone();
			} else if (l % 5 == 0) {
				cal.add(Calendar.YEAR, 1);
				Assert.assertTrue(Bytes.compareTo(Index.dataToBytes(icol, cal), Index.dataToBytes(icol, cal3)) < 0);
				cal3 = (Calendar) cal.clone();
			}
			cal.add(Calendar.HOUR, 1);
		}
	}
}
