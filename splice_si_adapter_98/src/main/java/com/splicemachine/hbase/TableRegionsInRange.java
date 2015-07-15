package com.splicemachine.hbase;

import org.apache.hadoop.hbase.HRegionLocation;
import java.io.IOException;
import java.util.List;

/**
 * Created by jleach on 7/15/15.
 */
public interface TableRegionsInRange {
    public List<HRegionLocation> getRegionsInRange(byte[] startKey, byte[] endKey, boolean reload) throws IOException;
    public HRegionLocation getRegionLocation(byte[] row) throws IOException;
}
