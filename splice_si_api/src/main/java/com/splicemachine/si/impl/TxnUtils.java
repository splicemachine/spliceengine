/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.impl;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;

import static com.splicemachine.si.constants.SIConstants.TRANSACTION_TABLE_BUCKET_COUNT;
/**
 * @author Scott Fines
 *         Date: 6/20/14
 */
public class TxnUtils {

	private TxnUtils(){}

	public static byte[] getRowKey(long txnId) {
		long beginTS = txnId & SIConstants.TRANSANCTION_ID_MASK;
		byte[] rowKey = new byte[9];
		rowKey[0] = (byte)((beginTS / SIConstants.TRASANCTION_INCREMENT) & (TRANSACTION_TABLE_BUCKET_COUNT-1));
		Bytes.longToBytes(beginTS, rowKey, 1);
		return rowKey;
	}

	private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
	public static String rowKeytoHbaseEscaped(byte[] rowKey) {
		char[] hexChars = new char[rowKey.length * 4];
		for (int j = 0; j < rowKey.length; j++) {
			int v = rowKey[j] & 0xFF;
			hexChars[j * 4] = '\\';
			hexChars[j * 4 + 1] = 'x';
			hexChars[j * 4 + 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 4 + 3] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars);
	}

	public static long txnIdFromRowKey(byte[] buffer, int rowOffset, int rowLength) {
		return Bytes.toLong(buffer, rowOffset + 1, rowLength - 1);
	}

	public static byte[] getOldRowKey(long txnId) {
		byte[] rowKey = new byte[9];
		rowKey[0] = (byte) (txnId & (TRANSACTION_TABLE_BUCKET_COUNT - 1));
		Bytes.longToBytes(txnId, rowKey, 1);
		return rowKey;
	}
}
