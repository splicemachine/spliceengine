package com.splicemachine.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;

public class SpliceMachineHBaseUtils {

	
	public static void getRegionStoreFiles() throws IOException {	
		/*
		HRegion region = null;
		Store store = region.getStore(null);
		Collection<StoreFile> storeFiles = store.getStorefiles();
		for (StoreFile sf: storeFiles) {
			Path path = sf.getPath();
			StoreFileScanner storeFileScanner = sf.createReader().getStoreFileScanner(false, false, false);
			KeyValue keyValue = storeFileScanner.next();
		}
		*/		
	}
	
}
