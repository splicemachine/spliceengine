package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public interface DataDelete extends DataMutation{
    void deleteColumn(DataCell dc);

    void deleteColumn(byte[] family,byte[] qualifier,long version);

    byte[] key();

    Iterable<DataCell> cells();
}
