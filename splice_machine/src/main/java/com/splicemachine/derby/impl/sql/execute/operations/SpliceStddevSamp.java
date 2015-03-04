package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
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
       if (result == null) {
           if (count > 1) {
               result = new Double(Math.sqrt(variance/(count-1)));
           } else {
               result = new Double(0);
           }
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

