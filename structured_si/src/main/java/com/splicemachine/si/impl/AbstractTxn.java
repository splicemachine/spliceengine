package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public abstract class AbstractTxn extends AbstractTxnView implements Txn {

		protected AbstractTxn(long txnId,
													long beginTimestamp,
													IsolationLevel isolationLevel) {
        super(txnId,beginTimestamp,isolationLevel);
		}

}
