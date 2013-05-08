package com.splicemachine.hbase.txn.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.txn.TxnConstants;

public class TxnTestUtils extends BaseTest {
	
	public static void putDataToTable(HTable htable, int totalRows, String txnID) throws IOException {
		putDataToTable(htable, 0, totalRows, txnID);
	}
	
	public static void putDataToTable(HTable htable, int startingRowNum, int totalRows, String txnID) throws IOException {
		byte[] family = SpliceConstants.DEFAULT_FAMILY.getBytes();
		char letter = 'A';
		Integer num = 111;

		boolean col7 = true;
		long col8 = 30000;
		double col9 = 1000000;
		List<Put> puts = new ArrayList<Put>();
		totalRows = startingRowNum + totalRows;
		for (; startingRowNum < totalRows; ++startingRowNum) {
			Put put = new Put(Bytes.toBytes("ROW" + startingRowNum));
			if (txnID != null)
				put.setAttribute(TxnConstants.TRANSACTION_ID, txnID.getBytes());
			put.add(family, COL1, Bytes.toBytes(letter));
			if (startingRowNum % 2 == 0) ++letter;
			if (startingRowNum % 4 == 0) col7 = true;
			else col7 = false;
			if (startingRowNum % 3 == 0) ++col8;
			put.add(family, COL2, Bytes.toBytes(num++));
			put.add(family, COL3, VAL1);
			put.add(family, COL4, VAL2);
			put.add(family, COL5, VAL3);
			put.add(family, COL6, VAL4);
			put.add(family, COL7, Bytes.toBytes(col7));
			put.add(family, COL8, Bytes.toBytes(col8));
			put.add(family, COL9, Bytes.toBytes(col9++));
			puts.add(put);
		}
		htable.put(puts);
	}
	
	/**
	 * Count the number of rows. Note that scan batch is set to 100.
	 * @throws IOException 
	 */
	public static int countRow(HTableInterface table, Integer startingRowNum, String txnID) throws IOException {
		int count = 0;
		Scan scan;
		if (startingRowNum != null)	
			scan = new Scan(Bytes.toBytes(startingRowNum));
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
	
	public static HTable getTestTable(HBaseAdmin admin, String tableName) throws Exception {
		if (!admin.tableExists(tableName)) {
			return createTable(admin, tableName); 
		} else {
			return new HTable(hbaseTestingUtility.getConfiguration(), tableName);
		}
	}
	
	public static HTable createTable(HBaseAdmin admin, String tableName) throws Exception {
		HTableDescriptor desc = new HTableDescriptor(tableName);
		desc.addFamily(new HColumnDescriptor(SpliceConstants.DEFAULT_FAMILY.getBytes(),
				SpliceConstants.DEFAULT_VERSIONS,
				compression,
				SpliceConstants.DEFAULT_IN_MEMORY,
				SpliceConstants.DEFAULT_BLOCKCACHE,
				SpliceConstants.DEFAULT_TTL,
				SpliceConstants.DEFAULT_BLOOMFILTER));
		admin.createTable(desc);
		return new HTable(admin.getConfiguration(), tableName);
	}
	
	 /**
     * put rows into table with given transaction id
     */
    public static void putSingleFamilyData(HTable table, String transactionID) throws Exception {
    	List<Put> puts = new ArrayList<Put>();
		for (int i=1; i<1001; ++i) {
	    	Put put = new Put(("ROW"+i).getBytes());
	    	put.setAttribute(TxnConstants.TRANSACTION_ID, transactionID.getBytes());
	    	put.add(DEFAULT_FAMILY_BYTES, COL1, VAL1);
	    	put.add(DEFAULT_FAMILY_BYTES, COL2, VAL2);
	    	puts.add(put);
		}
		table.put(puts);
    }
}
