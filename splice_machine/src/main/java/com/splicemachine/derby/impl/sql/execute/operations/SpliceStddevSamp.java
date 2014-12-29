package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

public class SpliceStddevSamp<K extends Double> extends SpliceUDAVariance<K> {

    private Double result;

    public SpliceStddevSamp() {
    }

    public void init() {
        super.init();
        result = null;
    }

    @Override
    public K terminate() {
        if (result == null) {
            if (count > 1) {
                result = Math.sqrt(variance / (count - 1));
            } else {
                result = 0d;
            }
        }
        return (K) result;
    }

    @Override
    public void add(DataValueDescriptor addend) throws StandardException {
        if (!addend.isNull()) {
            result = addend.getDouble();
        }
    }

}

