package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.Txn;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public abstract class AbstractTxn extends AbstractTxnView implements Txn{

    protected AbstractTxn(){

    }

    protected AbstractTxn(long txnId,
                          long beginTimestamp,
                          IsolationLevel isolationLevel){
        super(txnId,beginTimestamp,isolationLevel);
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException{
        throw new UnsupportedOperationException("Transactions cannot be serialized, only their views");
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException{
        throw new UnsupportedOperationException("Transactions cannot be serialized, only their views");
    }
}
