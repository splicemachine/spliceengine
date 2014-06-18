package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;

/**
 * @author Scott Fines
 *         Date: 6/23/14
 */
public class RolledBackTxn extends InheritingTxnView {

		public RolledBackTxn(long txnId){
			super(Txn.ROOT_TRANSACTION,txnId,txnId,null,false,false,false,false,false,false,-1l,-1l,State.ROLLEDBACK);
		}
}
