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

