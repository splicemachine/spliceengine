package com.splicemachine.constants.environment;

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SchemaConstants;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.constants.TxnConstants.TableEnv;

public class EnvUtils {
	private static Logger LOG = Logger.getLogger(EnvUtils.class);
	private static long FIRST_USER_TABLE_NUMBER = 1168;

	public static TableEnv getTableEnv(String tableName) {
		LOG.info("Checking table environment for " + tableName);
		if (tableName.equals(TxnConstants.TRANSACTION_TABLE))
			return TableEnv.TRANSACTION_TABLE;
		else if (tableName.equals(TxnConstants.TRANSACTION_LOG_TABLE))
			return TableEnv.TRANSACTION_LOG_TABLE;
		else if (tableName.endsWith(SchemaConstants.INDEX))
			return TableEnv.USER_INDEX_TABLE;
		else if (tableName.equals("-ROOT-"))
			return TableEnv.ROOT_TABLE;
		else if (tableName.equals(".META."))
			return TableEnv.META_TABLE;
		else if (tableName.equals(TxnConstants.TEMP_TABLE))
			return TableEnv.DERBY_SYS_TABLE;
		else {
			LOG.info("OTHER_TABLES table name= " + tableName+", with hard-cocded FIRST_USER_TABLE_NUMBER="+FIRST_USER_TABLE_NUMBER);
			try {
				long tableNumber = Long.parseLong(tableName);			
				if (tableNumber < FIRST_USER_TABLE_NUMBER) 
					return TableEnv.DERBY_SYS_TABLE;
			} catch (Exception e) {
				LOG.error("OTHER_TABLES table name is not a number. Need to check");
			}
			
			return TableEnv.USER_TABLE;
		}
	}
	
	public static String getRegionId(HRegion region) {
		return getRegionId(region.getRegionNameAsString());
	}
	
	public static String getRegionId(String regionName) {
		String[] tokens = regionName.split(",");
		if (tokens.length < 1)
			throw new RuntimeException("Invalid region name " + regionName);
		return tokens[tokens.length - 1];
	}
}
