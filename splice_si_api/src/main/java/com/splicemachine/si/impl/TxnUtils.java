/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
				rowKey[0] = (byte)(beginTS & (TRANSACTION_TABLE_BUCKET_COUNT-1));
				Bytes.longToBytes(beginTS, rowKey, 1);
				return rowKey;
		}

		public static long txnIdFromRowKey(byte[] buffer, int rowOffset, int rowLength) {
				return Bytes.toLong(buffer, rowOffset + 1, rowLength - 1);
		}
}
