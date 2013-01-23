package com.ir.hbase.test.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.ir.constants.HBaseConstants;
import com.ir.constants.TxnConstants;
import com.ir.hbase.test.idx.IdxTestUtils;

public class YarnTestUtils extends IdxTestUtils {

	public static void putData(HTable htable, String txnID, Configuration conf) throws Exception {
		char letter = 'A';
		Integer num = 111;

		boolean col7 = true;
		long col8 = 30000;
		double col9 = 1000000;

		for (int rowNum = 1; rowNum < 2501; ++rowNum) {
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
	public static void putData2(HTable htable, String txnID, Configuration conf) throws Exception {
		char letter = 'A';
		Integer num = 111;

		boolean col7 = true;
		long col8 = 30000;
		double col9 = 1000000;

		for (int rowNum = 1; rowNum < 2001; ++rowNum) {
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
	public static void putDataToDefaultFamily(HTable htable, String txnID, Configuration conf) throws Exception {
		char letter = 'A';
		Integer num = 111;

		boolean col7 = true;
		long col8 = 30000;
		double col9 = 1000000;

		for (int rowNum = 10001; rowNum < 20001; ++rowNum) {
			Put put = new Put(Bytes.toBytes("ROW" + rowNum));
			if (txnID != null)
				put.setAttribute(TxnConstants.TRANSACTION_ID, txnID.getBytes());
			put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, COL1, Bytes.toBytes(letter));
			if (rowNum % 2 == 0) ++letter;
			if (rowNum % 4 == 0) col7 = true;
			else col7 = false;
			if (rowNum % 3 == 0) ++col8;
			put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, COL2, Bytes.toBytes(num++));
			put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, COL3, VAL1);
			put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, COL4, VAL2);
			put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, COL5, VAL3);
			put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, COL6, VAL4);
			put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, COL7, Bytes.toBytes(col7));
			put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, COL8, Bytes.toBytes(col8));
			put.add(HBaseConstants.DEFAULT_FAMILY_BYTES, COL9, Bytes.toBytes(col9++));
			htable.put(put);
		}
	}
}
