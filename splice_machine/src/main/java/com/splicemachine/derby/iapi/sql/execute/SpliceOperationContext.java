package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.txn.TxnView;


/**
 * Represents the context of a SpliceOperation stack.
 * <p/>
 * This is primarily intended to ease the initialization interface by providing a single
 * wrapper object, instead of 400 different individual elements.
 *
 * @author Scott Fines
 *         Created: 1/18/13 9:18 AM
 */
public class SpliceOperationContext{
    private final GenericStorablePreparedStatement preparedStatement;
    private final Activation activation;
    private TxnView txn;
    private SConfiguration config;

    public SpliceOperationContext(Activation activation,
                                  GenericStorablePreparedStatement preparedStatement,
                                  TxnView txn,
                                  SConfiguration config){
        this.activation=activation;
        this.preparedStatement=preparedStatement;
        this.txn=txn;
        this.config = config;
    }

    public GenericStorablePreparedStatement getPreparedStatement(){
        return preparedStatement;
    }

    public Activation getActivation(){
        return activation;
    }

    public TxnView getTxn(){
        return txn;
    }

    public static SpliceOperationContext newContext(Activation a){
        return newContext(a,null,null); //TODO -sf- make this return a configuration
    }

    public static SpliceOperationContext newContext(Activation a,TxnView txn,SConfiguration config){
        if(txn==null){
            TransactionController te=a.getLanguageConnectionContext().getTransactionExecute();
            txn=((SpliceTransactionManager)te).getRawTransaction().getActiveStateTxn();
        }
        return new SpliceOperationContext(a,
                (GenericStorablePreparedStatement)a.getPreparedStatement(),
                txn,
                config);
    }

    public SConfiguration getSystemConfiguration(){
        throw new UnsupportedOperationException("IMPLEMENT: config may not be set!");
//        return config;
    }

    public LanguageConnectionContext getLanguageConnectionContext(){
        return activation.getLanguageConnectionContext();
    }
}
