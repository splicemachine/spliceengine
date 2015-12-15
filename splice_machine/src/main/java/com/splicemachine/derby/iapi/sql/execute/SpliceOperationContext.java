package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import org.apache.log4j.Logger;


/**
 * Represents the context of a SpliceOperation stack.
 *
 * This is primarily intended to ease the initialization interface by providing a single
 * wrapper object, instead of 400 different individual elements.
 *
 * @author Scott Fines
 * Created: 1/18/13 9:18 AM
 */
public class SpliceOperationContext {
    static final Logger LOG = Logger.getLogger(SpliceOperationContext.class);
    private final GenericStorablePreparedStatement preparedStatement;
    private final Activation activation;
	private TxnView txn;

    public SpliceOperationContext(Activation activation,
                                  GenericStorablePreparedStatement preparedStatement,
								  TxnView txn){
        this.activation = activation;
        this.preparedStatement = preparedStatement;
	    this.txn = txn;
    }

    public GenericStorablePreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public Activation getActivation() {
        return activation;
    }

		public TxnView getTxn() { return txn; }

		public static SpliceOperationContext newContext(Activation a){
				return newContext(a,null);
		}

    public static SpliceOperationContext newContext(Activation a,TxnView txn){
				if(txn==null){
						TransactionController te = a.getLanguageConnectionContext().getTransactionExecute();
						txn = ((SpliceTransactionManager) te).getRawTransaction().getActiveStateTxn();
				}
        return new SpliceOperationContext(
                a,
                (GenericStorablePreparedStatement)a.getPreparedStatement(),
                txn);
    }

}
