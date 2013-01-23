package com.ir.hbase.test.idx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.ir.constants.HBaseConstants;
import com.ir.constants.TxnConstants;
import com.ir.hbase.client.index.IndexTableStructure;
import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.client.structured.Family;
import com.ir.hbase.client.structured.TableStructure;
import com.ir.hbase.test.BaseTest;

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
		index1.setAddColumns(generateAddColumns());
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
	
	public static HTableDescriptor generateIndexTableDescriptor(HTableDescriptor desc) {
		IndexTableStructure its = new IndexTableStructure();
		its.addIndex(generateIndex1());
		its.addIndex(generateIndex2());
		return IndexTableStructure.setIndexStructure(desc, its);
	}
	public static HTableDescriptor generateIndexTableDescriptor() {
		return generateIndexTableDescriptor(generateNormalTableDescriptor());
	}
	public static HTableDescriptor generateStructuredTableDescriptor() {
		return generateStructuredTableDescriptor(null);
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

	public static HTableDescriptor generateStructuredTableDescriptor(HTableDescriptor desc) {
		return desc == null ? TableStructure.setTableStructure(generateNormalTableDescriptor(), generateTableStructure()) : TableStructure.setTableStructure(desc, generateTableStructure());
	}
	
	public static HTableDescriptor generateIdxStructuredTableDescriptor() {
		return generateStructuredTableDescriptor(generateIndexTableDescriptor(generateNormalTableDescriptor()));
	}
	
	public static HTableDescriptor generateIdxStructuredTableDescriptor(String tableName) {
		return generateStructuredTableDescriptor(generateIndexTableDescriptor(generateNormalTableDescriptor(tableName)));
	}
	
	public static HTableDescriptor generateNormalTableDescriptor() {
		return generateNormalTableDescriptor(regTable1);
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
	 * Put 10 rows 9 columns into table
	 * @param txnID null if without transaction
	 * @throws IOException
	 */
	public static void putDataToTable(HTableInterface htable, String txnID) throws IOException {
		char letter = 'A';
		Integer num = 111;

		boolean col7 = true;
		long col8 = 30000;
		double col9 = 1000000;

		for (int rowNum = 1; rowNum < 11; ++rowNum) {
			Put put = new Put(Bytes.toBytes("ROW" + rowNum));
			if (txnID != null)
				put.setAttribute(TxnConstants.TRANSACTION_ID, txnID.getBytes());
			put.add(FAMILY1, COL1, Bytes.toBytes(letter));
			if (rowNum % 2 == 0) ++letter;
			if (rowNum % 4 == 0) col7 = true;
			else col7 = false;
			if (rowNum % 3 == 0) ++col8;
			put.add(FAMILY1, COL2, Bytes.toBytes(num++));
			put.add(FAMILY2, COL3, VAL1);
			put.add(FAMILY2, COL4, VAL2);
			put.add(FAMILY3, COL5, VAL3);
			put.add(FAMILY3, COL6, VAL4);
			put.add(FAMILY4, COL7, Bytes.toBytes(col7));
			put.add(FAMILY4, COL8, Bytes.toBytes(col8));
			put.add(FAMILY4, COL9, Bytes.toBytes(col9++));
			htable.put(put);
		}
	}

	/**
	 * Count the number of rows. Note that scan batch is set to 100.
	 * @throws IOException 
	 */
	public static int countRowNum(HTableInterface table, String txnID) throws IOException {
		int count = 0;
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
		while(scanner.next() != null)
			++count;
		return count;
	}
	/**
	 * Access index table, check if there is a row key contains both str1 and str2
	 */
	public static boolean containsRow(String str1, String str2, HTableInterface table, String txnID) throws IOException {
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
		while((result = scanner.next()) != null) {
			if (Bytes.toString(result.getRow()).contains(str1) 
					&& Bytes.toString(result.getRow()).contains(str2))
				return true;
		}
		return false;
	}
	/**
	 * Check if there is an index row whose name contains both str1 and str2 has a column named family:column
	 */
	public static boolean containsColumn(String str1, String str2, byte[] family, byte[] column, HTableInterface table, String txnID) throws IOException {
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
		while((result = scanner.next()) != null) {
			if (Bytes.toString(result.getRow()).contains(str1) 
					&& Bytes.toString(result.getRow()).contains(str2)) {
				return result.containsColumn(family, column);
			}
		}
		return false;
	}

	/**
	 * Print content of table with or without transaction function. Use this to check rows in IndexTable are in correct order.
	 * @param table the table to be printed
	 * @param txnID set to null if access table without transaction function
	 * @throws IOException
	 */
	public static void printTable(HTableInterface table, String txnID) throws IOException {
		Scan scan = new Scan();
		scan.setBatch(10);
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
		while((result = scanner.next()) != null) {
			StringBuffer sbuffer = new StringBuffer();
			sbuffer.append(Bytes.toString(result.getRow()) + '\t');
			for (byte[] family : result.getMap().keySet()) {
				sbuffer.append(Bytes.toString(family) + ":");
				NavigableMap<byte[], byte[]> map = result.getFamilyMap(family);
				for (byte[] column : map.keySet()) {
					sbuffer.append(Bytes.toString(column) + " ");
					sbuffer.append(Bytes.toString(map.get(column)) + "  ");
				}
			}
			System.out.println("Print row in " + table.getTableDescriptor().getNameAsString() + ": " + sbuffer.toString());
		}
	}
	
}