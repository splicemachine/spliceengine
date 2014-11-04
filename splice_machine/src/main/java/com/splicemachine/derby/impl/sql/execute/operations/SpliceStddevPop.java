package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.error.StandardException;
import java.lang.Math;

public class SpliceStddevPop<K extends Double> extends SpliceUDAVariance<K>
{

    Double result;

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
               result = new Double(0);
           } else {
               result = new Double(Math.sqrt(variance/count));
           }
        }
        return (K) result;
    }

    @Override
    public void add (DataValueDescriptor addend) throws StandardException{
        result = addend.getDouble();
    }

}

