package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;

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

		public void setSavePointName(String savePointName) {
			this.savePointName = savePointName;
		}

		@Override
	    public String getSavePointName() {
	    	return savePointName;
	    }
}
