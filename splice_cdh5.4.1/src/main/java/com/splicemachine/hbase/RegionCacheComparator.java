package com.splicemachine.hbase;

import java.util.Comparator;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Simple Comparator that deferrs to the HRegionInfo comparator.  This is used to store sorted versions
 * of the regions in the cache.
 * 
 *
 */
@Deprecated
@SuppressFBWarnings(value = "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",justification = "Serialization unnecessary")
public class RegionCacheComparator implements Comparator<Pair<HRegionInfo,ServerName>>{

	@Override
	public int compare(Pair<HRegionInfo, ServerName> arg0,
			Pair<HRegionInfo, ServerName> arg1) {
		return arg0.getFirst().compareTo(arg1.getFirst());
	}

}
