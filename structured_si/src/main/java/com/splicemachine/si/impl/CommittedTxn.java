package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;

/**
 * Represents a "Committed transaction"--that is,
 * a transaction which when read was already committed.
 *
 * This is primarily useful for constructing stub transactions
 * when performing scans over commit timestamp columns.
 *
 * @author Scott Fines
 * Date: 6/23/14
 */
public class CommittedTxn extends InheritingTxnView {
		public CommittedTxn(long beginTimestamp, long endTimestamp){
			super(Txn.ROOT_TRANSACTION,beginTimestamp,beginTimestamp,Txn.ROOT_TRANSACTION.getIsolationLevel(),false,false,false,false,false,false,endTimestamp,endTimestamp, Txn.State.COMMITTED);
		}
}
