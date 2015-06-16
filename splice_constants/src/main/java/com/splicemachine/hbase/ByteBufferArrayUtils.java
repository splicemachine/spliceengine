package com.splicemachine.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.lucene.util.ArrayUtil;

public class ByteBufferArrayUtils {
   
    public static boolean matchingColumn(KeyValue keyValue, byte[] family, byte[] qualifier) {
    	return matchingFamily(keyValue,family)&&matchingQualifier(keyValue,qualifier);
    }

    public static boolean matchingFamily(KeyValue keyValue, byte[] family) {
    	if (family==null||keyValue==null || family.length != keyValue.getFamilyLength())
    		return false;
    	return ArrayUtil.equals(keyValue.getFamilyArray(), keyValue.getFamilyOffset(), family, 0, keyValue.getFamilyLength());
    }
    
    public static boolean matchingQualifier(KeyValue keyValue, byte[] qualifier) {
    	if (qualifier==null||keyValue==null||qualifier.length != keyValue.getQualifierLength())
    		return false;
    	return ArrayUtil.equals(keyValue.getQualifierArray(), keyValue.getQualifierOffset(), qualifier, 0, keyValue.getQualifierLength());
    }

    public static boolean matchingValue(KeyValue keyValue, byte[] value) {
    	if (value==null||keyValue==null || value.length != keyValue.getValueLength())
    		return false;
    	return ArrayUtil.equals(keyValue.getValueArray(), keyValue.getValueOffset(), value, 0, keyValue.getValueLength());
    }

	public static boolean matchingFamilyKeyValue(KeyValue keyValue, KeyValue other) {
		if (keyValue==null||other==null || keyValue.getFamilyLength()!=other.getFamilyLength())
			return false;
		return ArrayUtil.equals(keyValue.getFamilyArray(), keyValue.getFamilyOffset(), other.getFamilyArray(), other.getFamilyOffset(), other.getFamilyLength());
	}

	public static boolean matchingQualifierKeyValue(KeyValue keyValue, KeyValue other) {
		if (keyValue==null||other==null || keyValue.getQualifierLength()!=other.getQualifierLength())
			return false;
		return ArrayUtil.equals(keyValue.getQualifierArray(), keyValue.getQualifierOffset(), other.getQualifierArray(), other.getQualifierOffset(), other.getQualifierLength());
	}

	public static boolean matchingRowKeyValue(KeyValue keyValue, KeyValue other) {
		if (keyValue==null||other==null || keyValue.getRowLength()!=other.getRowLength())
			return false;
		return ArrayUtil.equals(keyValue.getRowArray(), keyValue.getRowOffset(), other.getRowArray(), other.getRowOffset(), other.getRowLength());
	}

    public static boolean matchingValueKeyValue(KeyValue keyValue, KeyValue other) {
		if (keyValue==null||other==null || keyValue.getValueLength()!=other.getValueLength())
			return false;
		return ArrayUtil.equals(keyValue.getValueArray(), keyValue.getValueOffset(), other.getValueArray(), other.getValueOffset(), other.getValueLength());
    }

	
	
}
