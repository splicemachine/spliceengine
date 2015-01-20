package com.splicemachine.si.api;

import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A Factory for creating transactionally aware operations (e.g. Puts, Scans, Deletes, Gets, etc.)
 *
 * @author Scott Fines
 * Date: 7/8/14
 */
public interface TxnOperationFactory {

		Put newPut(TxnView txn,byte[] rowKey) throws IOException;

		Scan newScan(TxnView txn);

		Scan newScan(TxnView txn, boolean isCountStar);

		Get newGet(TxnView txn,byte[] rowKey);

		Mutation newDelete(TxnView txn,byte[] rowKey) throws IOException;

		TxnView fromReads(OperationWithAttributes op) throws IOException;

		TxnView fromWrites(OperationWithAttributes op) throws IOException;

    void writeTxn(TxnView txn, ObjectOutput oo) throws IOException;

    TxnView readTxn(ObjectInput oi) throws IOException;

		byte[] encode(TxnView txn);

		TxnView decode(byte[] data, int offset,int length);

}
