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

