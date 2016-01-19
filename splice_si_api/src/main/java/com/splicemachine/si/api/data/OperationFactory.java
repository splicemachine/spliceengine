package com.splicemachine.si.api.data;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/18/16
 */
public interface OperationFactory{

    DataScan newScan();

    DataGet newGet(byte[] rowKey,DataGet previous);

    DataPut newPut(byte[] rowKey);

    DataPut newPut(ByteSlice slice);

    DataDelete newDelete(byte[] rowKey);

    DataCell newCell(byte[] key, byte[] family, byte[] qualifier, byte[] value);

    DataCell newCell(byte[] key, byte[] family, byte[] qualifier, long timestamp,byte[] value);

    void writeScan(DataScan scan, ObjectOutput out) throws IOException;

    DataScan readScan(ObjectInput in) throws IOException;

    DataResult newResult(List<DataCell> visibleColumns);

    DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp);
}
