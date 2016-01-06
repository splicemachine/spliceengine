package com.splicemachine.si.impl;

import com.splicemachine.access.api.NotServingPartitionException;
import org.apache.hadoop.hbase.NotServingRegionException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class HNotServingRegion extends NotServingRegionException implements NotServingPartitionException{
    public HNotServingRegion(){
    }

    public HNotServingRegion(String s){
        super(s);
    }

    public HNotServingRegion(byte[] s){
        super(s);
    }
}
