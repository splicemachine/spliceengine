package com.splicemachine.derby.utils;

import com.splicemachine.derby.hbase.SpliceDriver;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Scott Fines
 *         Date: 7/31/14
 */
public class GenericSpliceFunctions {

    private GenericSpliceFunctions(){} //don't instantiate utility class

    public static long LONG_UUID(){
        return SpliceDriver.driver().getUUIDGenerator().nextUUID();
    }

    public static void main(String...args) throws Exception{
        System.out.println(Bytes.toStringBinary(Bytes.toBytes(37)));
    }
}
