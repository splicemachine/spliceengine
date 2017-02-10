/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
