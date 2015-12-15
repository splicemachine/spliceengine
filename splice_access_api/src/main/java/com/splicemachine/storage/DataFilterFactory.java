package com.splicemachine.storage;

/**
 * Factory for creating different DataFilters. Each architecture is expected to provide an architecture
 * specific version of this.
 *
 * @author Scott Fines
 *         Date: 12/16/15
 */
public interface DataFilterFactory{
    /**
     * Filter rows based on whether or not the value at the specified family and qualifier is <em>equal</em>
     * to the specified value.
     *
     * @param family the family to check
     * @param qualifier the qualifier of the column to check
     * @param value the value to check
     * @return a DataFilter which performed the equality check.
     */
    DataFilter singleColumnEqualsValueFilter(byte[] family,byte[] qualifier,byte[] value);
}
