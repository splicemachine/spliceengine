package com.splicemachine.si.impl;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Scott Fines
 *         Date: 6/20/14
 */
public class TxnUtils {

		private TxnUtils(){}

		public static byte[] getRowKey(long txnId) {
				byte[] rowKey = new byte[9];
				rowKey[0] = (byte)(txnId & (SpliceConstants.TRANSACTION_TABLE_BUCKET_COUNT-1));
				BytesUtil.longToBytes(txnId, rowKey, 1);
				return rowKey;
		}

		public static long txnIdFromRowKey(byte[] buffer, int rowOffset, int rowLength) {
				return Bytes.toLong(buffer, rowOffset + 1, rowLength - 1);
		}
}
