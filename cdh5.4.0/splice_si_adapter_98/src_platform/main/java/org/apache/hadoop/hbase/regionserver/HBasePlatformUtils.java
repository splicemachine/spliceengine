package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Counter;

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
}
