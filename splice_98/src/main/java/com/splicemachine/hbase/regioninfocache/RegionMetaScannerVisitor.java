package com.splicemachine.hbase.regioninfocache;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Pair;
import java.io.IOException;

class RegionMetaScannerVisitor extends BaseRegionMetaScannerVisitor {
    RegionMetaScannerVisitor(byte[] updateTableName) {
    	super(updateTableName);
    }

	@Override
	public Pair<HRegionInfo, ServerName> parseCatalogResult(Result rowResult) throws IOException {
		return HRegionInfo.getHRegionInfoAndServerName(rowResult);
	}

}
