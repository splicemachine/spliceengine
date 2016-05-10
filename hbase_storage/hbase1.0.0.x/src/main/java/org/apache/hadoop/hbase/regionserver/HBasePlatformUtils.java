package org.apache.hadoop.hbase.regionserver;


import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import java.io.IOException;
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
		return region.getStores();
	}

    public static void flush(HRegion region) throws IOException {
        region.flushcache();
    }

    public static void bulkLoadHFiles(HRegion region, List<Pair<byte[], String>> copyPaths) throws IOException{
        region.bulkLoadHFiles(copyPaths,true);
    }
    public static long getMemstoreSize(HRegion region) {
        return region.getMemstoreSize().get();
    }

    public static long getReadpoint(HRegion region) {
        return region.getMVCC().memstoreReadPoint();
    }

    public static void validateClusterKey(String quorumAddress) throws IOException {
        ZKUtil.transformClusterKey(quorumAddress);
    }


}
