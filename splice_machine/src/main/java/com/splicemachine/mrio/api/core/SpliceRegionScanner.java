package com.splicemachine.mrio.api.core;

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
/**
 * Provides an interface for the region scanner that exposed the actual region as well.  This is 
 * required for transactional region interface as well as for keeping the iteration possible between
 * hbase 0.94 and 0.98.
 * 
 *
 */
public interface SpliceRegionScanner extends RegionScanner {
	public HRegion getRegion();
}
