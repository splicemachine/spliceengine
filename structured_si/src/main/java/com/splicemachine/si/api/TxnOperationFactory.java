package com.splicemachine.si.api;

import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * A Factory for creating transactionally aware operations (e.g. Puts, Scans, Deletes, Gets, etc.)
 *
 * @author Scott Fines
 * Date: 7/8/14
 */
public interface TxnOperationFactory {


		Put newPut(Txn txn,byte[] rowKey) throws IOException;

		Scan newScan(Txn txn);

		Scan newScan(Txn txn, boolean isCountStar);

		Get newGet(Txn txn,byte[] rowKey);

		Mutation newDelete(Txn txn,byte[] rowKey) throws IOException;

		Txn fromReads(OperationWithAttributes op) throws IOException;

		Txn fromWrites(OperationWithAttributes op) throws IOException;
}
