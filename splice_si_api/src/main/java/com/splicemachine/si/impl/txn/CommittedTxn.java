package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.txn.InheritingTxnView;

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
			super(Txn.ROOT_TRANSACTION,beginTimestamp,beginTimestamp,Txn.ROOT_TRANSACTION.getIsolationLevel(),false,false,false,false,endTimestamp,endTimestamp, Txn.State.COMMITTED);
		}
}
