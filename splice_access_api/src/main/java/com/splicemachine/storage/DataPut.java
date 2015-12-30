package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public interface DataPut extends DataMutation{

    void tombstone(long txnIdLong);

    void antiTombstone(long txnIdLong);

    void addCell(byte[] family, byte[] qualifier, long timestamp, byte[] value);

    void addCell(byte[] family, byte[] qualifier, byte[] value);

    byte[] key();

    Iterable<DataCell> cells();

    void addCell(DataCell kv);
}
