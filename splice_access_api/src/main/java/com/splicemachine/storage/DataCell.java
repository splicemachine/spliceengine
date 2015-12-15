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
     * Get a copy of this cell with the value specified. The resulting cell will have the same type
     * unless the type is affected by the new value.
     *
     * @param newValue the new value to hold
     * @return a copy of this cell with the new value (but with the same version and key)
     * @throws NullPointerException if {@code newValue ==null}
     */
    DataCell copyValue(byte[] newValue);

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
}

