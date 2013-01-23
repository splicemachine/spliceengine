package com.ir.hbase.index.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.ir.constants.HBaseConstants;
import com.ir.constants.SchemaConstants;
import com.ir.constants.TxnConstants;
import com.ir.hbase.client.index.IndexTableStructure;
import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.client.structured.Family;
import com.ir.hbase.client.structured.TableStructure;

public class IdxTestUtils extends BaseTest {

	public static List<Column> generateAddColumns() {
		List<Column> addColumns = new ArrayList<Column>();
		addColumns.add(column1);
		addColumns.add(column2);
		addColumns.add(column3);
		addColumns.add(column4);
		return addColumns;
	}
	
	public static Index generateIndex1() {
		Index index1 = new Index(INDEX1);
		index1.addIndexColumn(icolumn1);
		index1.addIndexColumn(icolumn2);
		return index1;
	}
	
	public static Index generateIndex2() {
		Index index2 = new Index(INDEX2);
		index2.addIndexColumn(icolumn3);
		index2.addIndexColumn(icolumn4);
		index2.addIndexColumn(icolumn5);
		index2.setAddColumns(generateAddColumns());
		return index2;
	}
	
	public static IndexTableStructure generateIndexTableStructure() {
		IndexTableStructure its = new IndexTableStructure();
		its.addIndex(generateIndex1());
		its.addIndex(generateIndex2());
		return its;
	}
	
	public static TableStructure generateTableStructure() {
		Family family1 = new Family(colfam1);
		Family family2 = new Family(colfam2);
		Family family3 = new Family(colfam3);
		Family family4 = new Family(colfam4);

		family1.addColumn(icolumn1);
		family1.addColumn(icolumn2);
		family2.addColumn(column1);
		family2.addColumn(column2);
		family3.addColumn(column3);
		family3.addColumn(column4);
		family4.addColumn(icolumn3);
		family4.addColumn(icolumn4);
		family4.addColumn(icolumn5);

		TableStructure ts = new TableStructure();
		ts.addFamily(family1);
		ts.addFamily(family2);
		ts.addFamily(family3);
		ts.addFamily(family4);
		return ts;
	}

	public static HTableDescriptor generateNormalTableDescriptor(String tableName) {
		HTableDescriptor desc = new HTableDescriptor(tableName);
		desc.addFamily(new HColumnDescriptor(FAMILY1));
		desc.addFamily(new HColumnDescriptor(FAMILY2));
		desc.addFamily(new HColumnDescriptor(FAMILY3));
		desc.addFamily(new HColumnDescriptor(FAMILY4));
		return desc;
	}
	
	public static HTableDescriptor generateDefaultTableDescriptor(String tableName) {
		HTableDescriptor desc = new HTableDescriptor(tableName);
		TableStructure ts = new TableStructure();
		ts.addFamily(new Family(HBaseConstants.DEFAULT_FAMILY));
		return TableStructure.setTableStructure(desc, ts);
	}

	/**
	 * Check if there is an index row whose name contains both str1 and str2 has a column named family:column
	 */
	public static boolean containsColumn(String indexName, byte[] family, byte[] column, HTableInterface table, String txnID) throws IOException {
		Scan scan = new Scan();
		scan.setBatch(100);
		if (txnID != null) {
			scan.setAttribute(TxnConstants.TRANSACTION_ID, txnID.getBytes());
			scan.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
					Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
		} else {
			scan.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
					Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_COMMITTED.toString()));
		}
		ResultScanner scanner = table.getScanner(scan);
		Result result;
		boolean contains = false;
		while((result = scanner.next()) != null) {
			if (Bytes.toString(result.getRow()).contains(indexName)) {
				contains = true;
				 if (!result.containsColumn(family, column))
					 return false;
			}
		}
		if (!contains)
			throw new RuntimeException("Table " + table.getTableDescriptor().getNameAsString() + " doesn't contain index " + indexName);
		return true;
	}
	
	public static void putSingleColumnToTable(HTable htable, int startRow, int endRow, String txnID) throws IOException {
		List<Put> puts = new ArrayList<Put>();
		for (; startRow <= endRow; ++startRow) {
			Put put = new Put(Bytes.toBytes("ROW" + startRow));
			put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), COL2, Bytes.toBytes(startRow));
			puts.add(put);
		}
		htable.put(puts);
	}
	
	public static void deleteSingleColumnFromTable(HTable htable, int startRow, int endRow, String txnID) throws IOException {
		List<Delete> deletes = new ArrayList<Delete>();
		for (; startRow <= endRow; ++startRow) {
			Delete delete = new Delete(Bytes.toBytes("ROW" + startRow));
			delete.deleteColumn(HBaseConstants.DEFAULT_FAMILY.getBytes(), COL2);
			deletes.add(delete);
		}
		htable.delete(deletes);
	}

	
	public static void putMultipleColumnsToTable(HTable htable, int startRow, int endRow, String txnID) throws IOException {
		byte[] family = HBaseConstants.DEFAULT_FAMILY.getBytes();
		char letter = 'A';
		Integer num = 111;

		String col3 = "AAAAA";
		boolean col7 = true;
		long col8 = 30000;
		double col9 = 1000000;
		
		List<Put> puts = new ArrayList<Put>();
		for (; startRow <= endRow; ++startRow) {
			Put put = new Put(Bytes.toBytes("ROW" + startRow));
			if (txnID != null)
				put.setAttribute(TxnConstants.TRANSACTION_ID, txnID.getBytes());
			put.add(family, COL1, Bytes.toBytes(letter));
			if (startRow % 2 == 0) { 
				++letter;
				col3 = col3.substring(0, 2) + (char)(col3.charAt(2) + 1) + col3.substring(3, 5);
			}
			if (startRow % 4 == 0) {
				col7 = true;
				col3 = (char)(col3.charAt(0) + 1) + col3.substring(1, 5);

			}
			else col7 = false;
			if (startRow % 3 == 0) {
				++col8;
				col3 = col3.substring(0, 1) + (char)(col3.charAt(1) + 1) + col3.substring(2, 5);
			}
			put.add(family, COL2, Bytes.toBytes(num++));
			put.add(family, COL3, Bytes.toBytes(col3));
			put.add(family, COL4, VAL2);
			put.add(family, COL5, VAL3);
			put.add(family, COL6, VAL4);
			put.add(family, COL7, Bytes.toBytes(col7));
			put.add(family, COL8, Bytes.toBytes(col8));
			put.add(family, COL9, Bytes.toBytes(col9++));
			puts.add(put);
		}
		htable.put(puts);
		col3 = col3.substring(0, 3) + (char)(col3.charAt(3) + 1) + col3.substring(4, 5);
	}
	
	/**
	 * Count the number of rows. Note that scan batch is set to 100.
	 * @throws IOException 
	 */
	public static int countRow(HTableInterface table, String startingRow, String txnID) throws IOException {
		int count = 0;
		Scan scan;
		if (startingRow != null)	
			scan = new Scan(Bytes.toBytes(startingRow));
		else 
			scan = new Scan();			
		scan.setCaching(100);
		if (txnID != null) {
			scan.setAttribute(TxnConstants.TRANSACTION_ID, txnID.getBytes());
			scan.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
					Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
		} else {
			scan.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
					Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_COMMITTED.toString()));
		}
		ResultScanner scanner = table.getScanner(scan);
		while(scanner.next() != null)
			++count;
		return count;
	}

	public static void refreshRegularTable1(HTableDescriptor desc) throws IOException {
		admin = hbaseTestingUtility.getHBaseAdmin();
		if (admin.tableExists(REGULAR_TABLE1)) {
			admin.disableTable(REGULAR_TABLE1);
			admin.deleteTable(REGULAR_TABLE1);
		}
		if (admin.tableExists(Bytes.toBytes(regTable1 + SchemaConstants.INDEX))) {
			admin.disableTable(Bytes.toBytes(regTable1 + SchemaConstants.INDEX));
			admin.deleteTable(Bytes.toBytes(regTable1 + SchemaConstants.INDEX));
		}
		admin.createTable(desc);
		htable = new HTable(hbaseTestingUtility.getConfiguration(), REGULAR_TABLE1);
		indexTable = new HTable(hbaseTestingUtility.getConfiguration(), Bytes.toBytes(regTable1 + SchemaConstants.INDEX));
	}
	
	public static void refreshRegularTable1() throws IOException {
		refreshRegularTable1(generateDefaultTableDescriptor(regTable1));
	}
	
}