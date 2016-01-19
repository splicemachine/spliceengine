package com.splicemachine.si.api.data;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;

import java.util.Comparator;
import java.util.List;

/**
 * Defines an abstraction over the construction and manipulate of HBase operations. Having this abstraction allows an
 * alternate lightweight store to be used instead of HBase (e.g. for rapid testing).
 */
public interface SDataLib{
    DataPut newDataPut(ByteSlice key);

    DataScan newDataScan();

    DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp);

    DataResult newResult(List<DataCell> visibleColumns);

    DataDelete newDataDelete(byte[] key);

    byte[] newRowKey(Object[] args);

    byte[] encode(Object value);

    <T> T decode(byte[] value,Class<T> type);

    <T> T decode(byte[] value,int offset,int length,Class<T> type);

}
