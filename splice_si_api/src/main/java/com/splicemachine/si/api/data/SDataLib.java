package com.splicemachine.si.api.data;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * Defines an abstraction over the construction and manipulate of HBase operations. Having this abstraction allows an
 * alternate lightweight store to be used instead of HBase (e.g. for rapid testing).
 */
public interface SDataLib<OperationWithAttributes,
        Data,
        Delete extends OperationWithAttributes,
        Get extends OperationWithAttributes,
        Put extends OperationWithAttributes,
        RegionScanner,
        Result,
        Scan extends OperationWithAttributes>{
    byte[] newRowKey(Object[] args);

    byte[] encode(Object value);

    <T> T decode(byte[] value,Class<T> type);

    <T> T decode(byte[] value,int offset,int length,Class<T> type);

    List<DataCell> listResult(Result result);

    Put newPut(byte[] key);

    DataPut newDataPut(ByteSlice key);

    void addKeyValueToPut(Put put,byte[] family,byte[] qualifier,long timestamp,byte[] value);

    Get newGet(byte[] key);

    void setGetTimeRange(Get get,long minTimestamp,long maxTimestamp);

    void setGetMaxVersions(Get get);

    Scan newScan();

    Scan newScan(byte[] startRowKey,byte[] endRowKey);

    DataScan newDataScan();

    void setScanTimeRange(Scan get,long minTimestamp,long maxTimestamp);

    void setScanMaxVersions(Scan get);

    Delete newDelete(byte[] rowKey);

    DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp);

    boolean singleMatchingColumn(Data element,byte[] family,byte[] qualifier);

    boolean singleMatchingFamily(Data element,byte[] family);

    boolean singleMatchingQualifier(Data element,byte[] qualifier);

    boolean matchingValue(Data element,byte[] value);

    boolean matchingRowKeyValue(Data element,Data other);

    Comparator getComparator();

    long getTimestamp(Data element);

    String getFamilyAsString(Data element);

    String getQualifierAsString(Data element);

    void setRowInSlice(Data element,ByteSlice slice);

    boolean isFailedCommitTimestamp(Data element);

    Data newTransactionTimeStampKeyValue(Data element,byte[] value);

    long getValueLength(Data element);

    long getValueToLong(Data element);

    byte[] getDataValue(Data element);

    byte[] getDataRow(Data element);

    byte[] getDataValueBuffer(Data element);

    byte[] getDataRowBuffer(Data element);

    int getDataRowOffset(Data element);

    int getDataRowlength(Data element);

    int getDataValueOffset(Data element);

    int getDataValuelength(Data element);

    int getLength(Data element);

    Data[] getDataFromResult(Result result);

    boolean regionScannerNext(RegionScanner regionScanner,List<Data> data) throws IOException;

    boolean regionScannerNextRaw(RegionScanner regionScanner,List<Data> data) throws IOException;

    void setAttribute(OperationWithAttributes operation,String name,byte[] value);

    byte[] getAttribute(OperationWithAttributes operation,String attributeName);

    Data matchKeyValue(Data[] kvs,byte[] columnFamily,byte[] qualifier);

    Data matchDataColumn(Data[] kvs);

    DataResult newResult(List<DataCell> visibleColumns);

    DataDelete newDataDelete(byte[] key);

}
