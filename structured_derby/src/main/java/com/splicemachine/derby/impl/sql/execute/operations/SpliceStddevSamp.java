package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.error.StandardException;
import java.lang.Math;

public class SpliceStddevSamp<K extends Double> extends SpliceUDAVariance<K>
{

    Double result;

    public SpliceStddevSamp() {

    }

    public void init() {
        super.init();
        result = null;
    }

    @Override
    public K terminate() {
       if (result == null && count > 1) {
            result = new Double(Math.sqrt(variance/(count-1)));
        }
        return (K) result;
    }

    @Override
    public void add (DataValueDescriptor addend) throws StandardException{
        if (!addend.isNull()){
            result = addend.getDouble();
        }
    }

}

