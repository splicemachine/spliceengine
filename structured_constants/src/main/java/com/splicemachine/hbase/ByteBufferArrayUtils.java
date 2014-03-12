package com.splicemachine.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.lucene.util.ArrayUtil;

public class ByteBufferArrayUtils {

    // TODO: jc - Can we use org.apache.hadoop.hbase.CellUtil here instead?

    public static boolean matchingColumn(Cell keyValue, byte[] family, byte[] qualifier) {
    	return matchingFamily(keyValue,family)&&matchingQualifier(keyValue,qualifier);
    }

    public static boolean matchingFamily(Cell keyValue, byte[] family) {
    	if (family==null||keyValue==null || family.length != keyValue.getFamilyLength())
    		return false;
    	return ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getFamilyOffset(), family, 0,
                                keyValue.getFamilyLength());
    }
    
    public static boolean matchingQualifier(Cell keyValue, byte[] qualifier) {
    	if (qualifier==null||keyValue==null||qualifier.length != keyValue.getQualifierLength())
    		return false;
    	return ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getQualifierOffset(), qualifier, 0, keyValue.getQualifierLength());
    }

    public static boolean matchingValue(Cell keyValue, byte[] value) {
    	if (value==null||keyValue==null || value.length != keyValue.getValueLength())
    		return false;
    	return ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getValueOffset(), value, 0, keyValue.getValueLength());
    }

	public static boolean matchingFamilyKeyValue(Cell keyValue, Cell other) {
		if (keyValue==null||other==null || keyValue.getFamilyLength()!=other.getFamilyLength())
			return false;
		return ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getFamilyOffset(), CellUtils.getBuffer(other), other.getFamilyOffset(), other.getFamilyLength());
	}

	public static boolean matchingQualifierKeyValue(Cell keyValue, Cell other) {
		if (keyValue==null||other==null || keyValue.getQualifierLength()!=other.getQualifierLength())
			return false;
		return ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getQualifierOffset(), CellUtils.getBuffer(other), other.getQualifierOffset(), other.getQualifierLength());
	}

	public static boolean matchingRowKeyValue(Cell keyValue, Cell other) {
		if (keyValue==null||other==null || keyValue.getRowLength()!=other.getRowLength())
			return false;
		return ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getRowOffset(), CellUtils.getBuffer(other), other.getRowOffset(), other.getRowLength());
	}

    public static boolean matchingValueKeyValue(Cell keyValue, Cell other) {
		if (keyValue==null||other==null || keyValue.getValueLength()!=other.getValueLength())
			return false;
		return ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getValueOffset(), CellUtils.getBuffer(other), other.getValueOffset(), other.getValueLength());
    }

}
