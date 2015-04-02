package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.exception.ErrorState;

/**
 * Collection of static functions for use as derby stored functions.
 *
 * @author Scott Fines
 *         Date: 4/2/15
 */
public class NumericFunctions{

    public static long SCALAR_POW(long base,int scale) throws StandardException{
        double pow=Math.pow(base,scale);
        if(Double.isNaN(pow)||Double.isInfinite(pow))
            throw ErrorState.LANG_OUTSIDE_RANGE_FOR_DATATYPE.newException();
        return (long)pow;
    }

    public static double POW(double base, double scale) throws StandardException{
        double pow=Math.pow(base,scale);
        if(Double.isNaN(pow)||Double.isInfinite(pow))
            throw ErrorState.LANG_OUTSIDE_RANGE_FOR_DATATYPE.newException();
        return pow;
    }
}
