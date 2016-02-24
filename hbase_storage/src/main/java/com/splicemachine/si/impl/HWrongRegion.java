package com.splicemachine.si.impl;

import com.splicemachine.access.api.WrongPartitionException;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class HWrongRegion extends WrongRegionException implements WrongPartitionException{
    public HWrongRegion(){ }

    public HWrongRegion(String s){
        super(s);
    }
}
