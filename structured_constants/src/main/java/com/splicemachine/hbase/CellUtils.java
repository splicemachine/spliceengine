package com.splicemachine.hbase;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;

/**
 * @author Scott Fines
 *         Date: 2/14/14
 */
public class CellUtils {

		private CellUtils(){}
		
		public static boolean singleMatchingColumn(Cell keyValue, byte[] family, byte[] qualifier) {
			return singleMatchingFamily(keyValue,family) && singleMatchingQualifier(keyValue,qualifier);
		}
		
		public static boolean singleMatchingFamily(Cell keyValue, byte[] family) {
				return keyValue.getFamilyArray()[keyValue.getFamilyOffset()] == family[0];
//			return getBuffer(keyValue)[keyValue.getFamilyOffset()] == family[0];
		}

		public static boolean singleMatchingQualifier(Cell keyValue, byte[] qualifier) {
				return keyValue.getQualifierArray()[keyValue.getQualifierOffset()] == qualifier[0];
//			return getBuffer(keyValue)[keyValue.getQualifierOffset()] == qualifier[0];
		}

		public static boolean matchingValue(Cell keyValue, byte[] value) {
				return ByteBufferArrayUtils.matchingValue(keyValue, value);
		}

		public static boolean matchingFamilyKeyValue(Cell keyValue, Cell other) {
				return ByteBufferArrayUtils.matchingFamilyKeyValue(keyValue, other);
		}
		public static boolean matchingQualifierKeyValue(Cell keyValue, Cell other) {
				return ByteBufferArrayUtils.matchingQualifierKeyValue(keyValue, other);
		}
		public static boolean matchingRowKeyValue(Cell keyValue, Cell other) {
				return ByteBufferArrayUtils.matchingRowKeyValue(keyValue, other);
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

    // =========================
    // Moved from KeyValueUtils
    // =========================

    public static Cell matchKeyValue(Iterable<Cell> kvs, byte[] columnFamily, byte[] qualifier) {
        for (Cell kv : kvs) {
            if (CellUtils.singleMatchingColumn(kv, columnFamily, qualifier))
                return kv;
        }
        return null;
    }

    public static Cell matchKeyValue(Cell[] kvs, byte[] columnFamily, byte[] qualifier) {
        for (Cell kv : kvs) {
            if (CellUtils.singleMatchingColumn(kv, columnFamily, qualifier))
                return kv;
        }
        return null;
    }

    public static Cell matchDataColumn(Cell[] kvs) {
        return matchKeyValue(kvs, SpliceConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES);
    }

    public static Cell matchDataColumn(List<Cell> kvs) {
        return matchKeyValue(kvs, SpliceConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES);
    }

		public static boolean matchingColumn(Cell kv, byte[] family, byte[] qualifier) {
				return CellUtil.matchingFamily(kv, family) && CellUtil.matchingQualifier(kv,qualifier);
		}

}
