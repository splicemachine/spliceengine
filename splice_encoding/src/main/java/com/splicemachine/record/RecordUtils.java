package com.splicemachine.record;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

/**
 * Created by jleach on 1/2/17.
 */
public class RecordUtils {


    public int computeSetFields (int maximumValue) {



        return UnsafeRow.calculateBitSetWidthInBytes(maximumValue);
    }


}
