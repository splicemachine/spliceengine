package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.STable;
import org.apache.hadoop.hbase.regionserver.HRegion;

public class HbRegion implements STable {
    final HRegion region;

    public HbRegion(HRegion region) {
        this.region = region;
    }
}
