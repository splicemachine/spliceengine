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

package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 12/14/15
 */
public interface DataCell extends Comparable<DataCell>{

    byte[] valueArray();

    int valueOffset();

    int valueLength();

    byte[] keyArray();

    int keyOffset();

    int keyLength();

    CellType dataType();

    DataCell getClone();

    boolean matchesFamily(byte[] family);

    boolean matchesQualifier(byte[] family,byte[] dataQualifierBytes);

    long version();

    long valueAsLong();

    /**
     * Get a copy of this cell with the value and cell type specified.
     *
     * @param newValue the new value to hold
     * @param newCellType the new cell type to use
     * @return a copy of this cell with the new value (but with the same version and key)
     * @throws NullPointerException if {@code newValue ==null}
     */
    DataCell copyValue(byte[] newValue,CellType newCellType);

    /**
     * @return the size of this DataCell on disk.
     */
    int encodedLength();

    byte[] value();

    byte[] family();

    byte[] qualifier();

    byte[] key();

    byte[] qualifierArray();

    int qualifierOffset();

    long familyLength();
}

