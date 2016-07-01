package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 7/3/14
 */
public class ActiveWriteTxn extends AbstractTxnView{
    private TxnView parentTxn;
    private boolean additive;

    public ActiveWriteTxn(){
        super();
    }

    public ActiveWriteTxn(long txnId,
                          long beginTimestamp,
                          TxnView parentTxn,
                          boolean additive,
                          Txn.IsolationLevel isolationLevel){
        super(txnId,beginTimestamp,isolationLevel);
        this.parentTxn=parentTxn;
        this.additive=additive;
    }


    @Override
    public long getCommitTimestamp(){
        return -1l;
    }

    @Override
    public long getEffectiveCommitTimestamp(){
        return -1l;
    }

    @Override
    public long getGlobalCommitTimestamp(){
        return -1l;
    }

    @Override
    public TxnView getParentTxnView(){
        return parentTxn;
    }

    @Override
    public long getParentTxnId(){
        return parentTxn.getParentTxnId();
    }

    @Override
    public Txn.State getState(){
        return Txn.State.ACTIVE;
    }

    @Override
    public boolean allowsWrites(){
        return true;
    }

    @Override
    public boolean isAdditive(){
        return additive;
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException{
        super.readExternal(input);
        additive=input.readBoolean();
        parentTxn=(TxnView)input.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException{
        super.writeExternal(output);
        output.writeBoolean(additive);
        output.writeObject(parentTxn);
    }

    public ActiveWriteTxn getReadUncommittedActiveTxn() {
        return new ActiveWriteTxn(txnId,getBeginTimestamp(),parentTxn,additive, Txn.IsolationLevel.READ_UNCOMMITTED);
    }

}
