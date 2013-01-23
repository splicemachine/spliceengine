package com.ir.hbase.endpoint.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.ir.constants.TxnConstants;
import com.ir.hbase.endpoint.CountGroupByEndpointProtocol;
import com.ir.hbsae.test.ServerBaseTest;

public class CountGroupByEndPointTest extends ServerBaseTest {
	public static String tableName1 = "Endpoint_Test_Table1";
	public static String tableName2 = "Endpoint_Test_Table2";

	public static HTable table1; //one column per family table
	public static HTable table2; //single family table
	
	@Test
	public void createTable() throws Exception {
		if (!admin.tableExists(tableName1)) {
			HTableDescriptor desc1 = new HTableDescriptor(tableName1);
			desc1.addFamily(new HColumnDescriptor(FAMILY1));
			desc1.addFamily(new HColumnDescriptor(FAMILY2));
			desc1.addFamily(new HColumnDescriptor(FAMILY3));
			desc1.addFamily(new HColumnDescriptor(FAMILY4));
			desc1.addFamily(new HColumnDescriptor(FAMILY5));
			desc1.addFamily(new HColumnDescriptor(FAMILY6));
			desc1.addFamily(new HColumnDescriptor(FAMILY7));
			desc1.addFamily(new HColumnDescriptor(FAMILY8));
			desc1.addFamily(new HColumnDescriptor(FAMILY9));
			admin.createTable(desc1);
			table1 = new HTable(tableName1);
			putData1(table1, 1, 100000, null, conf);
			table1.close();
		}
		if (!admin.tableExists(tableName2)) {
			HTableDescriptor desc2 = new HTableDescriptor(tableName2);
			desc2.addFamily(new HColumnDescriptor(FAMILY1));
			admin.createTable(desc2);
			table2 = new HTable(tableName2);
			putData2(table2, 1, 100000, null, conf);
			table2.close();
		}
	}
	
	@Test
	public void splitTable() throws Exception {
		admin.split(tableName1, "87500");
	}
	/**
	 * Test Endpoint on table with one column per family.
	 * @throws Exception
	 */
	@Test
	public void testEndpoint1() throws Exception {
		table1 = new HTable(tableName1);
		System.out.println();
		Map<Integer, Long> result = null;
		List<Long> time = new ArrayList<Long>();
		for (int i=0; i<10; i++) {
			long start = System.currentTimeMillis();
			result = getCountGroupBy(table1, FAMILY1, COL1);
			time.add(System.currentTimeMillis() - start);
		}
		for (Map.Entry<Integer, Long> entry : result.entrySet()) {
			System.out.println(entry.getKey() + "   " + entry.getValue());
		}
		System.out.print("Running time:");
		long sum = 0;
		for (long t : time) {
			sum += t;
			System.out.print("\t" + t);
		}
		System.out.print("\nAverage: " + sum/time.size());
	}
	/**
	 * Test Endpoint on table with single family.
	 */
	@Test
	public void testEndpoint2() throws Exception {
		table2 = new HTable(tableName2);
		System.out.println();
		Map<Integer, Long> result = null;
		List<Long> time = new ArrayList<Long>();
		for (int i=0; i<10; i++) {
			long start = System.currentTimeMillis();
			result = getCountGroupBy(table2, FAMILY1, COL1);
			time.add(System.currentTimeMillis() - start);
		}
		for (Map.Entry<Integer, Long> entry : result.entrySet()) {
			System.out.println(entry.getKey() + "   " + entry.getValue());
		}
		System.out.print("Running time:");
		long sum = 0;
		for (long t : time) {
			sum += t;
			System.out.print("\t" + t);
		}
		System.out.print("\nAverage: " + sum/time.size());
	}
	
	@Test
	public void dropTable() throws Exception {
		if (admin.tableExists(tableName1)) {
			admin.disableTable(tableName1.getBytes());
			admin.deleteTable(tableName1.getBytes());
		}
		if (admin.tableExists(tableName2)) {
			admin.disableTable(tableName2.getBytes());
			admin.deleteTable(tableName2.getBytes());
		}
	}
	
	public static Map<Integer, Long> getCountGroupBy(HTable table, final byte[] fam, final byte[] col) {
		Map<Integer, Long> result = new HashMap<Integer, Long>();
		try {
			Map<byte[], Map<Integer, Long> > counts = table.coprocessorExec(CountGroupByEndpointProtocol.class, null, null, new Batch.Call<CountGroupByEndpointProtocol, Map<Integer, Long>>() {
				@Override
				public Map<Integer, Long> call(CountGroupByEndpointProtocol instance)
						throws IOException {
					return instance.getCount(fam, col);
				}
			});
			for (Map<Integer, Long> count : counts.values()) {
				for (Map.Entry<Integer, Long> entry : count.entrySet()) {
					Integer key = entry.getKey();
					Long value = entry.getValue();
					if (result.containsKey(key)) {
						result.put(key, result.get(key) + value);
					} else {
						result.put(key, value);
					}
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return result;
	}
	/**
	 * Put data in one column per family. 
	 */
	public static void putData1(HTable htable, int startRow, int EndRow, String txnID, Configuration conf) throws Exception {
		Integer groupKey = 1;
		Integer num = 111;

		boolean col7 = true;
		long col8 = 30000;
		double col9 = 1000000;

		for (int rowNum = startRow; rowNum < EndRow + 1; ++rowNum) {
			Put put = new Put(Bytes.toBytes(Integer.toString(rowNum)));
			if (txnID != null)
				put.setAttribute(TxnConstants.TRANSACTION_ID, txnID.getBytes());
			put.add(FAMILY1, COL1, Bytes.toBytes(groupKey));
			if (rowNum % 5000 == 0) ++groupKey;
			if (rowNum % 4 == 0) col7 = true;
			else col7 = false;
			if (rowNum % 3 == 0) ++col8;
			put.add(FAMILY2, COL2, Bytes.toBytes(num++));
			put.add(FAMILY3, COL3, VAL1);
			put.add(FAMILY4, COL4, VAL2);
			put.add(FAMILY5, COL5, VAL3);
			put.add(FAMILY6, COL6, VAL4);
			put.add(FAMILY7, COL7, Bytes.toBytes(col7));
			put.add(FAMILY8, COL8, Bytes.toBytes(col8));
			put.add(FAMILY9, COL9, Bytes.toBytes(col9++));
			htable.put(put);
		}
	}
	/**
	 * Put data in all columns of a single family
	 */
	public static void putData2(HTable htable, int startRow, int EndRow, String txnID, Configuration conf) throws Exception {
		Integer groupKey = 1;
		Integer num = 111;

		boolean col7 = true;
		long col8 = 30000;
		double col9 = 1000000;

		for (int rowNum = startRow; rowNum < EndRow + 1; ++rowNum) {
			Put put = new Put(Bytes.toBytes(Integer.toString(rowNum)));
			if (txnID != null)
				put.setAttribute(TxnConstants.TRANSACTION_ID, txnID.getBytes());
			put.add(FAMILY1, COL1, Bytes.toBytes(groupKey));
			if (rowNum % 5000 == 0) ++groupKey;
			if (rowNum % 4 == 0) col7 = true;
			else col7 = false;
			if (rowNum % 3 == 0) ++col8;
			put.add(FAMILY1, COL2, Bytes.toBytes(num++));
			put.add(FAMILY1, COL3, VAL1);
			put.add(FAMILY1, COL4, VAL2);
			put.add(FAMILY1, COL5, VAL3);
			put.add(FAMILY1, COL6, VAL4);
			put.add(FAMILY1, COL7, Bytes.toBytes(col7));
			put.add(FAMILY1, COL8, Bytes.toBytes(col8));
			put.add(FAMILY1, COL9, Bytes.toBytes(col9++));
			htable.put(put);
		}
	}
}
