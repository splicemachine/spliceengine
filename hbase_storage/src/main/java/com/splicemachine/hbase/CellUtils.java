/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.hbase;

import java.util.List;

import com.splicemachine.utils.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import static com.splicemachine.si.constants.SIConstants.DEFAULT_FAMILY_BYTES;
import static com.splicemachine.si.constants.SIConstants.PACKED_COLUMN_BYTES;

/**
 * @author Scott Fines
 *         Date: 2/14/14
 */
public class CellUtils {

		private CellUtils(){}

		/**
		 * Matches a column based upon the ASSUMPTION that the family and qualifier are both only a single
		 * byte each. This lets us avoid doing a bunch of math when we know that the family and qualifier are
		 * just a single byte.
		 *
		 * DO NOT USE THIS IF THE FAMILY OR QUALIFIER ARE NOT of length 1!!!
		 *
		 * @param keyValue the keyValue to Check
		 * @param family the family to check. Must be a single byte
		 * @param qualifier the qualifier to check. Must be a single byte
		 * @return true if the cell matches both the family AND the qualifier.
		 */
		public static boolean singleMatchingColumn(Cell keyValue, byte[] family, byte[] qualifier) {
			return singleMatchingFamily(keyValue,family) && singleMatchingQualifier(keyValue,qualifier);
		}

		/**
		 * Determines if the cell belongs to the specified family, in an efficient manner.
		 *
		 * Note: This should ONLY be used when the specified family is KNOWN to be a single byte. If it isn't
		 * a single byte, you'll probably get incorrect answers.
		 *
		 * @param keyValue the cell to check
		 * @param family the family to check. Must be a single byte
		 * @return true if the cell belongs to the specified family.
		 */
		public static boolean singleMatchingFamily(Cell keyValue, byte[] family) {
				assert family!=null && family.length==1: "The specified family is not of length 1. Use Cellutil.";
				return keyValue.getFamilyArray()[keyValue.getFamilyOffset()] == family[0];
		}

		/**
		 * Determines if the cell belongs to the specified family, in an efficient manner.
		 *
		 * Note: This should ONLY be used when the specified family is KNOWN to be a single byte. If it isn't
		 * a single byte, you'll probably get incorrect answers.
		 *
		 * @param keyValue the cell to check
		 * @param qualifier the family to check. Must be a single byte
		 * @return true if the cell belongs to the specified family.
		 */
		public static boolean singleMatchingQualifier(Cell keyValue, byte[] qualifier) {
				assert qualifier!=null: "Qualifiers should not be null";
				assert qualifier.length==1: "Qualifiers should be of length 1 not " + qualifier.length + " value --" + Bytes.toString(qualifier) + "--";
				return keyValue.getQualifierArray()[keyValue.getQualifierOffset()] == qualifier[0];
		}

		public static boolean matchingValue(Cell keyValue, byte[] value) {
				return CellByteBufferArrayUtils.matchingValue(keyValue, value);
		}

		public static boolean matchingFamilyKeyValue(Cell keyValue, Cell other) {
				return CellByteBufferArrayUtils.matchingFamilyKeyValue(keyValue, other);
		}
		public static boolean matchingQualifierKeyValue(Cell keyValue, Cell other) {
				return CellByteBufferArrayUtils.matchingQualifierKeyValue(keyValue, other);
		}
		public static boolean matchingRowKeyValue(Cell keyValue, Cell other) {
				return CellByteBufferArrayUtils.matchingRowKeyValue(keyValue, other);
		}

		public static Cell newKeyValue(Cell keyValue, byte[] value) {
            return new KeyValue(getBuffer(keyValue), keyValue.getRowOffset(), keyValue.getRowLength(),
                                getBuffer(keyValue), keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                                getBuffer(keyValue), keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                                keyValue.getTimestamp(), KeyValue.Type.Put, value, 0, value == null ? 0 : value.length);
        }

		public static Cell newKeyValue(byte[] rowKey, byte[] family, byte[] qualifier, Long timestamp, byte[] value) {
				return new KeyValue(rowKey, family, qualifier, timestamp, value);
		}


    public static byte[] getBuffer(Cell keyValue) {
        // TODO: jc- not really sure getRowArray() gives back the same thing as getBuffer() did
//        return keyValue.getRowArray();
        return ((KeyValue)keyValue).getBuffer();
    }

    public static int getLength(Cell keyValue) {
        if (keyValue == null)
            return 0;
        return getBuffer(keyValue).length;
    }

    public static int getTimestampOffset(Cell keyValue) {
        // TODO: jc - timestamp value is now a dedicated long value in Cell (see Cell#getTimestamp()). Keeping
        // this cast to preserve our current packed-row semantics. We should consider accommodating the new Cell
        // interface
        return ((KeyValue)keyValue).getTimestampOffset();
    }

    public static boolean isLatestTimestamp(Cell keyValue){
        return ((KeyValue)keyValue).isLatestTimestamp();
    }

    // =========================
    // Moved from KeyValueUtils
    // =========================

    public static Cell matchKeyValue(Iterable<Cell> kvs, byte[] columnFamily, byte[] qualifier) {
        for (Cell kv : kvs) {
            if (CellUtils.matchingColumn(kv, columnFamily, qualifier))
                return kv;
        }
        return null;
    }

    public static Cell matchKeyValue(Cell[] kvs, byte[] columnFamily, byte[] qualifier) {
        for (Cell kv : kvs) {
            if (CellUtils.matchingColumn(kv, columnFamily, qualifier))
                return kv;
        }
        return null;
    }

    public static Cell matchDataColumn(Cell[] kvs) {
        return matchKeyValue(kvs, DEFAULT_FAMILY_BYTES, PACKED_COLUMN_BYTES);
    }

    public static Cell matchDataColumn(List<Cell> kvs) {
        return matchKeyValue(kvs, DEFAULT_FAMILY_BYTES, PACKED_COLUMN_BYTES);
    }

	public static boolean matchingColumn(Cell kv, byte[] family, byte[] qualifier) {
			return CellUtil.matchingFamily(kv, family) && CellUtil.matchingQualifier(kv,qualifier);
	}

	/**
	 * Returns true if the specified KeyValue is contained by the specified range.
	 */
	public static boolean isKeyValueInRange(Cell kv, Pair<byte[], byte[]> range) {
		byte[] kvBuffer = kv.getRowArray(); // TODO JL SAR
		int rowKeyOffset = kv.getRowOffset();
		short rowKeyLength = kv.getRowLength();
		byte[] start = range.getFirst();
		byte[] stop = range.getSecond();
		return (start.length == 0 || Bytes.compareTo(start, 0, start.length, kvBuffer, rowKeyOffset, rowKeyLength) <= 0) &&
				(stop.length == 0 || Bytes.compareTo(stop, 0, stop.length, kvBuffer, rowKeyOffset, rowKeyLength) >= 0);
	}

	public static String toHex(byte[] bytes) {
		if (bytes == null) return "NULL";
		if (bytes.length == 0) return "";
		return Bytes.toHex(bytes);
	}
}