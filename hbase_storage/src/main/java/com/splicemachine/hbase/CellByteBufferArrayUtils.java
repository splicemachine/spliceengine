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
