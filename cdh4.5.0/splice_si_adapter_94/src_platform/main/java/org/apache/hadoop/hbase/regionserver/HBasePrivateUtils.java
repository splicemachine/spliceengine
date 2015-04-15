package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.master.MasterServices;

public class HBasePrivateUtils {

	public static CatalogTracker getCatalogTracker(MasterServices masterServices) {
		return masterServices.getCatalogTracker();
	}
}
