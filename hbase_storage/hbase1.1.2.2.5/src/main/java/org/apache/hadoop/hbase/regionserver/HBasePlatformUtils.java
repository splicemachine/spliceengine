/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
        return region.getMVCC().memstoreReadPoint();
    }

    public static void validateClusterKey(String quorumAddress) throws IOException {
        ZKConfig.validateClusterKey(quorumAddress);
    }

    public static boolean scannerEndReached(ScannerContext scannerContext) {
        scannerContext.setSizeLimitScope(ScannerContext.LimitScope.BETWEEN_ROWS);
        scannerContext.incrementBatchProgress(1);
        scannerContext.incrementSizeProgress(100l);
        return scannerContext.setScannerState(ScannerContext.NextState.BATCH_LIMIT_REACHED).hasMoreValues();
    }


}
