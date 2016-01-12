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
public interface SDataLib<OperationWithAttributes,
        Data,
        Get extends OperationWithAttributes,
        Scan extends OperationWithAttributes>{
    byte[] newRowKey(Object[] args);

    byte[] encode(Object value);

    <T> T decode(byte[] value,Class<T> type);

    <T> T decode(byte[] value,int offset,int length,Class<T> type);

    DataPut newDataPut(ByteSlice key);

    Get newGet(byte[] key);

    void setGetTimeRange(Get get,long minTimestamp,long maxTimestamp);

    void setGetMaxVersions(Get get);

    Scan newScan();

    DataScan newDataScan();

    void setScanTimeRange(Scan get,long minTimestamp,long maxTimestamp);

    void setScanMaxVersions(Scan get);

    DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp);

    boolean singleMatchingColumn(Data element,byte[] family,byte[] qualifier);

    boolean singleMatchingQualifier(Data element,byte[] qualifier);

    boolean matchingValue(Data element,byte[] value);

    Comparator getComparator();

    long getTimestamp(Data element);

    String getFamilyAsString(Data element);

    String getQualifierAsString(Data element);

    void setRowInSlice(Data element,ByteSlice slice);

    boolean isFailedCommitTimestamp(Data element);

    Data newTransactionTimeStampKeyValue(Data element,byte[] value);

    long getValueLength(Data element);

    long getValueToLong(Data element);

    byte[] getDataValueBuffer(Data element);

    byte[] getDataRowBuffer(Data element);

    int getDataRowOffset(Data element);

    int getDataRowlength(Data element);

    int getDataValueOffset(Data element);

    int getDataValuelength(Data element);

    int getLength(Data element);

    void setAttribute(OperationWithAttributes operation,String name,byte[] value);

    byte[] getAttribute(OperationWithAttributes operation,String attributeName);

    Data matchKeyValue(Data[] kvs,byte[] columnFamily,byte[] qualifier);

    Data matchDataColumn(Data[] kvs);

    DataResult newResult(List<DataCell> visibleColumns);

    DataDelete newDataDelete(byte[] key);

}
