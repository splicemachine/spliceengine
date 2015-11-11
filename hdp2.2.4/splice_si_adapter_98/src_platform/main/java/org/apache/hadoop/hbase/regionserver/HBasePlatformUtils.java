package org.apache.hadoop.hbase.regionserver;

import com.splicemachine.collections.NavigableCastingSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.cliffc.high_scale_lib.Counter;

import java.util.NavigableSet;

public class HBasePlatformUtils{
	public static NavigableSet<Cell> getKvset(HStore store) {
		return new NavigableCastingSet<>(store.memstore.kvset,KeyValue.COMPARATOR);
	}

	public static NavigableSet<Cell> getSnapshot(HStore store) {
		return new NavigableCastingSet<>(store.memstore.snapshot,KeyValue.COMPARATOR);
	}

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

}
