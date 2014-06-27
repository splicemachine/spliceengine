package com.splicemachine.si.coprocessors;

import com.splicemachine.concurrent.LongStripedSynchronizer;
import com.splicemachine.constants.SIConstants;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author Scott Fines
 *         Date: 6/19/14
 */
public class TxnLifecycleEndpoint extends BaseEndpointCoprocessor implements TxnLifecycleProtocol {

		private LongStripedSynchronizer<ReadWriteLock> lockStriper
						= LongStripedSynchronizer.stripedReadWriteLock(SIConstants.transactionlockStripes,false);

		@Override
		public void beginTransaction(long txnId, byte[] packedTxn) throws IOException {
		}

		@Override
		public void elevateTransaction(long txnId, byte[] newDestinationTable) throws IOException {

		}

		@Override
		public byte[] beginChildTransaction(long parentTxnId, byte[] packedChildTxn) throws IOException {
				return new byte[0];
		}

		@Override
		public long commit(long txnId) throws IOException {
				return 0;
		}

		@Override
		public void rollback(long txnId) throws IOException {

		}

		@Override
		public boolean keepAlive(long txnId) throws IOException {
				return false;
		}

		@Override
		public byte[] getTransaction(long txnId) throws IOException {
				return new byte[0];
		}
}
