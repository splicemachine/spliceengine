package com.splicemachine.si.api;

import com.splicemachine.hbase.KVPair;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;
import java.util.Collection;

/**
 * Interface for writing data to an individual region or table
 * in a transactional manner.
 *
 * The purpose of this interface is primarily to hide implementation details(
 * like forward-rollers and Constraint checkers) of the SI write
 * framework in an efficient manner.
 *
 * @author Scott Fines
 * Date: 6/27/14
 */
public interface TransactionalWriter {

		OperationStatus[] processKvBatch(byte[] family, byte[] qualifier,
																		 Collection<KVPair> mutations,
																		 long txnId) throws IOException;

		OperationStatus[] processKvBatch(byte[] family, byte[] qualifier,
																		 Collection<KVPair> mutations,
																		 Txn txn) throws IOException;
}
