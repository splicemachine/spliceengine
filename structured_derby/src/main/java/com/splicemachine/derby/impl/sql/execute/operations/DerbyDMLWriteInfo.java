package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.actions.WriteCursorConstantOperation;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Created on: 10/4/13
 */
public class DerbyDMLWriteInfo implements DMLWriteInfo {
    private transient Activation activation;


    @Override
    public void initialize(SpliceOperationContext opCtx) {
        this.activation = opCtx.getActivation();
    }

    @Override
    public ConstantAction getConstantAction() {
        return activation.getConstantAction();
    }

    @Override
    public FormatableBitSet getPkColumns() {
        return fromIntArray(getPkColumnMap());
    }

    @Override
    public int[] getPkColumnMap() {
        ConstantAction action = getConstantAction();
        return ((WriteCursorConstantOperation)action).getPkColumns();
    }

    @Override
    public long getConglomerateId() {
        return ((WriteCursorConstantOperation)getConstantAction()).getConglomerateId();
    }

		@Override
    public SpliceObserverInstructions buildInstructions(SpliceOperation operation) {
        return SpliceObserverInstructions.create(activation,operation,new SpliceRuntimeContext());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    }

    public static FormatableBitSet fromIntArray(int[] values){
        if(values ==null) return null;
        FormatableBitSet fbt = new FormatableBitSet(values.length);
        for(int value:values){
            fbt.grow(value);
            fbt.set(value-1);
        }
        return fbt;
    }
}
