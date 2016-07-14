/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.ErrorState;

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
