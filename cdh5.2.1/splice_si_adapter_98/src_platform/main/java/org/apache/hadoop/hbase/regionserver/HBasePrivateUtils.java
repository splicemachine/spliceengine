package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.master.MasterServices;

public class HBasePrivateUtils {
	public static KeyValueSkipListSet getKvset(HStore store) {
		return ((DefaultMemStore) store.memstore).kvset;
	}

	public static KeyValueSkipListSet getSnapshot(HStore store) {
		return ((DefaultMemStore) store.memstore).snapshot;
	}

	public static CatalogTracker getCatalogTracker(MasterServices masterServices) {
		return masterServices.getCatalogTracker();
	}
}
