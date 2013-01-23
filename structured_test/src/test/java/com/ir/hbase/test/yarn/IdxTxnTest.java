package com.ir.hbase.test.yarn;

import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.ir.constants.HBaseConstants;
import com.ir.constants.TxnConstants;
import com.ir.hbase.client.SchemaManager;
import com.ir.hbase.client.structured.Family;
import com.ir.hbase.client.structured.TableStructure;
import com.ir.hbase.coprocessor.SchemaUtils;
import com.ir.hbase.test.idx.IdxTestUtils;
import com.ir.hbase.txn.TransactionManager;
import com.ir.hbase.txn.TransactionState;
import com.ir.hbase.txn.logger.TxnLogger;
import com.ir.hbase.txn.logger.LogConstants.LogRecordType;

public class IdxTxnTest extends YarnBaseTest {
	public static String tableName1 = "TestTable1";
	public static String tableName2 = "TestTable2";
	public static String tableName3 = "tableName3";
	
	public static HTable table1;
	public static HTable table2;
	public static HTable table3;
	
	public static HTable txnTable;

	@Test
	public void testCreateTable() throws Exception {
		admin.createTable(YarnTestUtils.generateNormalTableDescriptor(tableName1));
		admin.createTable(YarnTestUtils.generateNormalTableDescriptor(tableName2));
		admin.createTable(YarnTestUtils.generateDefaultTableDescriptor(tableName3));

		table1 = new HTable(tableName1);
		table2 = new HTable(tableName2);
		table3 = new HTable(tableName3);
		YarnTestUtils.putData(table1, null, conf);
		YarnTestUtils.putData(table2, null, conf);
		YarnTestUtils.putDataToDefaultFamily(table3, null, conf);
		table1.close();
		table2.close();
		table3.close();

	}
	@Test
	public void testTxnLog1() throws Exception {
		admin.createTable(YarnTestUtils.generateNormalTableDescriptor(tableName1));
		table1 = new HTable(tableName1);
		TransactionManager tm = new TransactionManager(conf);
		TransactionState ts = tm.beginTransaction();
		YarnTestUtils.putData(table1, ts.getTransactionID(), conf);
		YarnTestUtils.putData2(table1, null, conf);
		printZK();
	}
	
	@Test
	public void testTxnLog2() throws Exception {
		TransactionManager tm = new TransactionManager(conf);
		tm.prepareCommit(new TransactionState("/transaction/txn-16909@Johns-Mac-Mini.local0000000000"));
	}
	
	@Test
	public void testTxnLog3() throws Exception {
		TransactionManager tm = new TransactionManager(conf);
		tm.tryCommit(new TransactionState("/transaction/txn-16909@Johns-Mac-Mini.local0000000000"));
	}
	
	@Test
	public void testSplit1() throws Exception {
		admin.createTable(YarnTestUtils.generateNormalTableDescriptor(tableName1));
		table1 = new HTable(tableName1);
		TransactionManager tm = new TransactionManager(conf);
		TransactionState ts = tm.beginTransaction();
		YarnTestUtils.putData(table1, ts.getTransactionID(), conf);
		YarnTestUtils.putData2(table1, null, conf);
		admin.split(tableName1, "ROW500");
		printZK();
	}
	@Test
	public void testSplit2() throws Exception {
		TransactionManager tm = new TransactionManager(conf);
		tm.prepareCommit(new TransactionState("/transaction/txn-9529@Johns-Mac-Mini.local0000000000"));
	}
	@Test
	public void testSplit3() throws Exception {
		TransactionManager tm = new TransactionManager(conf);
		tm.tryCommit(new TransactionState("/transaction/txn-9529@Johns-Mac-Mini.local0000000000"));
	}
	
	@Test
	public void addIndex1() throws Exception {
		sm.addIndexToTable(tableName1, YarnTestUtils.generateIndex1());
		sm.addIndexToTable(tableName2, YarnTestUtils.generateIndex1());
		sm.addIndexToTable(tableName3, YarnTestUtils.generateIndex1());
	}
	@Test
	public void addIndex2() throws Exception {
		sm.addIndexToTable(tableName2, YarnTestUtils.generateIndex2());
		sm.addIndexToTable(tableName3, YarnTestUtils.generateIndex2());
	}
	@Test
	public void putMoreData() throws Exception {
		table1 = new HTable(tableName1);
		table2 = new HTable(tableName2);
		table3 = new HTable(tableName3);
		YarnTestUtils.putData2(table1, null, conf);
		YarnTestUtils.putData2(table2, null, conf);
		YarnTestUtils.putData2(table3, null, conf);
		table1.close();
		table2.close();
		table3.close();
	}
	@Test
	public void testDeleteIndex() throws Exception {
		sm.deleteIndexFromTable(tableName1, INDEX1);
		sm.deleteIndexFromTable(tableName2, INDEX2);
		sm.deleteIndexFromTable(tableName3, INDEX2);
	}
	@Test
	public void testDropTable() throws Exception {
		admin.disableTable("REGULAR_TABLE1");
		admin.deleteTable("REGULAR_TABLE1");
	}

	@Test
	public void printZK() throws Exception {
		for (String schemaPath : rzk.getChildren("/", false)) {
			schemaPath = "/" + schemaPath;
			System.out.println(schemaPath);
			for (String table : rzk.getChildren(schemaPath, false)) {
				System.out.println("   " + table);
				for (String family : rzk.getChildren(SchemaUtils.getTablePath(schemaPath, table), false)) {
					System.out.println("      " + family);
					for (String column : rzk.getChildren(SchemaUtils.getFamilyPath(schemaPath, table, family), false)) {
						System.out.println("         " + column);
					}
				}
			}
		}
	}
	
}
