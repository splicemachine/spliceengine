package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.txn.InheritingTxnView;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @author Scott Fines
 *         Date: 6/23/14
 */
@SuppressFBWarnings("SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION")
public class RolledBackTxn extends InheritingTxnView {

		public RolledBackTxn(long txnId){
			super(Txn.ROOT_TRANSACTION,txnId,txnId,null,false,false,false,false,-1l,-1l, Txn.State.ROLLEDBACK);
		}
}
