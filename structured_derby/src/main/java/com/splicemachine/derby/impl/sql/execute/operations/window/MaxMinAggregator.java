package com.splicemachine.derby.impl.sql.execute.operations.window;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 7/22/14.
 */
public class MaxMinAggregator extends SpliceGenericWindowFunction {


    public void accumulate(DataValueDescriptor addend, Object g) throws StandardException {

    }

    public void merge(ExecAggregator inputAggregator) throws StandardException {

    }

    public DataValueDescriptor getResult() throws StandardException {
        return null;
    }

    public ExecAggregator newAggregator() {
        return null;
    }

    public boolean didEliminateNulls() {
        return false;
    }

    public int getTypeFormatId() {
        return -1;
    }

    public void writeExternal(ObjectOutput out) throws IOException {

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }
}
