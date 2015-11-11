package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Counter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

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

	public static NavigableSet<Cell> getKvset(HStore store) {
		return ((DefaultMemStore) store.memstore).cellSet;
	}

	public static NavigableSet<Cell> getSnapshot(HStore store) {
		return ((DefaultMemStore) store.memstore).snapshot;
	}


    public static Map<byte[],Store> getStores(HRegion region) {
        HashMap<byte[], Store> returnedStore = new HashMap();
        List<Store> stores = region.getStores();
        for (Store store: stores) {
            returnedStore.put(store.getColumnFamilyName().getBytes(),store);
        }
        return returnedStore;
    }

}
