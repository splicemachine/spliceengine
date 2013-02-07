package com.splicemachine.si.utils;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.datanucleus.store.valuegenerator.UUIDHexGenerator;

import com.splicemachine.constants.HBaseConstants;

public class SIUtils {
	public static final byte[] SNAPSHOT_ISOLATION = "_snapshotIsolation".getBytes();
	protected static UUIDHexGenerator gen = new UUIDHexGenerator("Splice", null);
	
	public static HTableDescriptor generateSIHtableDescriptor(String tableName) {
		HTableDescriptor descriptor = new HTableDescriptor(tableName);
		HColumnDescriptor snapShot = new HColumnDescriptor(SNAPSHOT_ISOLATION,
				Integer.MAX_VALUE,
				HBaseConstants.DEFAULT_COMPRESSION,
				HBaseConstants.DEFAULT_IN_MEMORY,
				true,
				HBaseConstants.DEFAULT_TTL,
				HBaseConstants.DEFAULT_BLOOMFILTER);
		HColumnDescriptor attributes = new HColumnDescriptor(HBaseConstants.DEFAULT_FAMILY.getBytes(),
				HBaseConstants.DEFAULT_VERSIONS,
				HBaseConstants.DEFAULT_COMPRESSION,
				HBaseConstants.DEFAULT_IN_MEMORY,
				HBaseConstants.DEFAULT_BLOCKCACHE,
				HBaseConstants.DEFAULT_TTL,
				HBaseConstants.DEFAULT_BLOOMFILTER);
		descriptor.addFamily(snapShot);		
		descriptor.addFamily(attributes);
		return descriptor;
	}
	
	public static String getUniqueTransactionID(){
		return gen.next().toString();			
	}
}
