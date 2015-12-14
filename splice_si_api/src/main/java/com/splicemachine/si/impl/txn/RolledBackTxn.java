package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.txn.InheritingTxnView;

/**
 * @author Scott Fines
 *         Date: 6/23/14
 */
public class RolledBackTxn extends InheritingTxnView {

		public RolledBackTxn(long txnId){
			super(Txn.ROOT_TRANSACTION,txnId,txnId,null,false,false,false,false,-1l,-1l, Txn.State.ROLLEDBACK);
		}
}
