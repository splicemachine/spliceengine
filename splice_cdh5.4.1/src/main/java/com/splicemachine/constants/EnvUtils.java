package com.splicemachine.constants;

import com.splicemachine.constants.SpliceConstants.TableEnv;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

public class EnvUtils {
	private static Logger LOG = Logger.getLogger(EnvUtils.class);
	private static final long FIRST_USER_TABLE_NUMBER = 1328;

    public static TableEnv getTableEnv(RegionCoprocessorEnvironment e) {
        return EnvUtils.getTableEnv(e.getRegion().getTableDesc().getTableName());
    }

    public static TableEnv getTableEnv(TableName tableName) {
        SpliceLogUtils.trace(LOG,"Checking table environment for %s",tableName);
        if (!tableName.getNamespaceAsString().equals(SpliceConstants.spliceNamespace))
            if (tableName.getQualifierAsString().equals("-ROOT-"))
                return TableEnv.ROOT_TABLE;
            else if (isMetaOrNamespaceTable(tableName))
                return TableEnv.META_TABLE;
            else {
                return TableEnv.HBASE_TABLE;
            }
        else if (tableName.getQualifierAsString().equals(SpliceConstants.TRANSACTION_TABLE))
            return TableEnv.TRANSACTION_TABLE;
        else {
			try {
				long tableNumber = Long.parseLong(tableName.getQualifierAsString());
				if (tableNumber <= FIRST_USER_TABLE_NUMBER)
					return TableEnv.DERBY_SYS_TABLE;
			} catch (Exception e) {
                SpliceLogUtils.debug(LOG,tableName+" is not a number");
			}
			return TableEnv.USER_TABLE;
		}
	}

    public static boolean isMetaOrNamespaceTable(TableName tableName) {
        String qualifier = tableName.getQualifierAsString();
        return "hbase:meta".equals(qualifier)
                || "meta".equals(qualifier)
                || "hbase:namespace".equals(qualifier)
                || "namespace".equals(qualifier)
                || ".META.".equals(qualifier);
    }

    public static String getRegionId(HRegion region) {
		return getRegionId(region.getRegionInfo().getRegionNameAsString());
	}
	
	public static String getRegionId(String regionName) {
		String[] tokens = regionName.split(",");
		if (tokens.length < 1)
			throw new RuntimeException("Invalid region name " + regionName);
		return tokens[tokens.length - 1];
	}

}
