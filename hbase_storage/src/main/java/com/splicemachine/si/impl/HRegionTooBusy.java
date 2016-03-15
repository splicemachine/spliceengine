package com.splicemachine.si.impl;

import com.splicemachine.access.api.RegionBusyException;
import org.apache.hadoop.hbase.RegionTooBusyException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class HRegionTooBusy extends RegionTooBusyException implements RegionBusyException {
    public HRegionTooBusy(){ }

    public HRegionTooBusy(String s){
        super(s);
    }
}
