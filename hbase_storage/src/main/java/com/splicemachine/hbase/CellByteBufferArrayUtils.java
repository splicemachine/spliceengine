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

import org.apache.hadoop.hbase.Cell;
import org.apache.lucene.util.ArrayUtil;

public class CellByteBufferArrayUtils {
	public static boolean matchingColumn(Cell keyValue, byte[] family, byte[] qualifier) {
    	return matchingFamily(keyValue,family) && matchingQualifier(keyValue,qualifier);
    }

    public static boolean matchingFamily(Cell keyValue, byte[] family) {
        return !(family == null || keyValue == null || family.length != keyValue.getFamilyLength()) &&
                ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getFamilyOffset(), family, 0,
                                 keyValue.getFamilyLength());
    }
    
    public static boolean matchingQualifier(Cell keyValue, byte[] qualifier) {
        return !(qualifier == null || keyValue == null || qualifier.length != keyValue.getQualifierLength()) &&
                ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getQualifierOffset(), qualifier, 0, keyValue.getQualifierLength());
    }

    public static boolean matchingValue(Cell keyValue, byte[] value) {
        return !(value == null || keyValue == null || value.length != keyValue.getValueLength()) && ArrayUtil.equals
                (CellUtils.getBuffer(keyValue), keyValue.getValueOffset(), value, 0, keyValue.getValueLength());
    }

	public static boolean matchingFamilyKeyValue(Cell keyValue, Cell other) {
        return !(keyValue == null || other == null || keyValue.getFamilyLength() != other.getFamilyLength()) &&
                ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getFamilyOffset(),
                                 CellUtils.getBuffer(other), other.getFamilyOffset(), other.getFamilyLength());
    }

	public static boolean matchingQualifierKeyValue(Cell keyValue, Cell other) {
        return !(keyValue == null || other == null || keyValue.getQualifierLength() != other.getQualifierLength()) &&
                ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getQualifierOffset(),
                                 CellUtils.getBuffer(other), other.getQualifierOffset(), other.getQualifierLength());
    }

	public static boolean matchingRowKeyValue(Cell keyValue, Cell other) {
        return !(keyValue == null || other == null || keyValue.getRowLength() != other.getRowLength()) &&
                ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getRowOffset(), CellUtils.getBuffer(other),
                                 other.getRowOffset(), other.getRowLength());
    }

    public static boolean matchingValueKeyValue(Cell keyValue, Cell other) {
        return !(keyValue == null || other == null || keyValue.getValueLength() != other.getValueLength()) &&
                ArrayUtil.equals(CellUtils.getBuffer(keyValue), keyValue.getValueOffset(),
                                 CellUtils.getBuffer(other), other.getValueOffset(), other.getValueLength());
    }
}
