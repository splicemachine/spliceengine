package org.apache.hadoop.hbase.regionserver;


import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBasePlatformUtils{
	public static void updateWriteRequests(HRegion region, long numWrites) {
		Counter writeRequestsCount = region.writeRequestsCount;
		if (writeRequestsCount != null)
			writeRequestsCount.add(numWrites);
	}

	public static void updateReadRequests(HRegion region, long numReads) {
		Counter readRequestsCount = region.readRequestsCount;
		if (readRequestsCount != null)
			readRequestsCount.add(numReads);
	}

	public static Map<byte[],Store> getStores(HRegion region) {
        List<Store> stores = region.getStores();
        HashMap<byte[],Store> storesMap = new HashMap<>();
        for (Store store: stores) {
            storesMap.put(store.getFamily().getName(),store);
        }
		return storesMap;
	}

    public static void flush(HRegion region) throws IOException {
        region.flushcache(false,false);
    }

    public static void bulkLoadHFiles(HRegion region, List<Pair<byte[], String>> copyPaths) throws IOException{
        // Is Null LISTENER Correct TODO Jun
        region.bulkLoadHFiles(copyPaths,true,null);
    }
    public static long getMemstoreSize(HRegion region) {
        return region.getMemstoreSize();
    }


    public static long getReadpoint(HRegion region) {
        return region.getMVCC().getReadPoint();
    }


    public static void validateClusterKey(String quorumAddress) throws IOException {
        ZKConfig.validateClusterKey(quorumAddress);
    }
}
