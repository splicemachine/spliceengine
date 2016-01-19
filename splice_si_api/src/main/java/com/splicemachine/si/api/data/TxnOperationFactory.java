package com.splicemachine.si.api.data;

import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.*;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A Factory for creating transactionally aware operations (e.g. Puts, Scans, Deletes, Gets, etc.)
 *
 * @author Scott Fines
 *         Date: 7/8/14
 */
public interface TxnOperationFactory{

    DataScan newDataScan(TxnView txn);

    DataGet newDataGet(TxnView txn,byte[] rowKey,DataGet previous);

    TxnView fromReads(Attributable op) throws IOException;

    TxnView fromWrites(Attributable op) throws IOException;

    TxnView fromWrites(byte[] data,int off,int length) throws IOException;

    TxnView fromReads(byte[] data,int off,int length) throws IOException;

    TxnView readTxn(ObjectInput oi) throws IOException;

    void writeTxn(TxnView txn,ObjectOutput out) throws IOException;

    void writeScan(DataScan scan, ObjectOutput out) throws IOException;

    DataScan readScan(ObjectInput in) throws IOException;

    byte[] encode(TxnView txn);

    void encodeForReads(Attributable attributable,TxnView txn, boolean isCountStar);

    void encodeForWrites(Attributable attributable,TxnView txn) throws IOException;

    TxnView decode(byte[] data,int offset,int length);

    DataPut newDataPut(TxnView txn,byte[] key) throws IOException;

    DataMutation newDataDelete(TxnView txn,byte[] key) throws IOException;

    DataCell newDataCell(byte[] key,byte[] family,byte[] qualifier,byte[] value);
}
