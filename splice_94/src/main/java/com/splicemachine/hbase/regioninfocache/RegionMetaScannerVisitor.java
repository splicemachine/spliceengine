package com.splicemachine.hbase.regioninfocache;

import com.google.common.collect.Sets;
import com.splicemachine.hbase.RegionCacheComparator;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkNotNull;

class RegionMetaScannerVisitor extends BaseRegionMetaScannerVisitor {
    RegionMetaScannerVisitor(byte[] updateTableName) {
    	super(updateTableName);
    }

	@Override
	public Pair<HRegionInfo, ServerName> parseCatalogResult(Result rowResult) throws IOException {
		return MetaReader.parseCatalogResult(rowResult);
	}

}
