package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

public class SpliceStddevPop<K extends Double> extends SpliceUDAVariance<K> {

    private Double result;

    public SpliceStddevPop() {
    }

    public void init() {
        super.init();
        result = null;
    }

    @Override
    public K terminate() {
        if (result == null) {
            if (count == 0) {
                result = 0d;
            } else {
                result = Math.sqrt(variance / count);
            }
        }
        return (K) result;
    }

    @Override
    public void add(DataValueDescriptor addend) throws StandardException {
        result = addend.getDouble();
    }

}

