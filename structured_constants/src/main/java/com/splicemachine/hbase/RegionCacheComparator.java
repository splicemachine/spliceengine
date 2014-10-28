package com.splicemachine.hbase;

import java.util.Comparator;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Simple Comparator that deferrs to the HRegionInfo comparator.  This is used to store sorted versions
 * of the regions in the cache.
 * 
 *
 */
public class RegionCacheComparator implements Comparator<Pair<HRegionInfo,ServerName>>{

	@Override
	public int compare(Pair<HRegionInfo, ServerName> arg0,
			Pair<HRegionInfo, ServerName> arg1) {
		return arg0.getFirst().compareTo(arg1.getFirst());
	}

}
