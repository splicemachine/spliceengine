package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.Txn;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public abstract class AbstractTxn extends AbstractTxnView implements Txn {

		protected AbstractTxn() {
			
		}
	
		protected AbstractTxn(long txnId,
													long beginTimestamp,
													IsolationLevel isolationLevel) {
        super(txnId,beginTimestamp,isolationLevel);
		}

		protected String savePointName = null;

}
