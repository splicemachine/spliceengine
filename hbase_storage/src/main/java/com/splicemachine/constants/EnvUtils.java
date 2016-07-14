package com.splicemachine.constants;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.si.data.hbase.coprocessor.TableType;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

public class EnvUtils {
	private static Logger LOG = Logger.getLogger(EnvUtils.class);
    // NOTE: JC - this constant is also defined in DataDictionary. When adding a new sys table, this
    // number will need to be increased in BOTH places.
	private static final long FIRST_USER_TABLE_NUMBER = 1458;

    public static TableType getTableType(SConfiguration config,RegionCoprocessorEnvironment e) {
        return EnvUtils.getTableType(config,e.getRegion().getTableDesc().getTableName());
    }

    public static TableType getTableType(SConfiguration config,TableName tableName) {
        SpliceLogUtils.trace(LOG,"Checking table environment for %s",tableName);
        if (!tableName.getNamespaceAsString().equals(config.getNamespace())) {
            if(tableName.getQualifierAsString().equals("-ROOT-"))
                return TableType.ROOT_TABLE;
            else if(isMetaOrNamespaceTable(tableName))
                return TableType.META_TABLE;
            else{
                return TableType.HBASE_TABLE;
            }
        }else if (tableName.getQualifierAsString().equals(HConfiguration.TRANSACTION_TABLE))
            return TableType.TRANSACTION_TABLE;
        else if(tableName.getQualifierAsString().equals(HConfiguration.TENTATIVE_TABLE))
            return TableType.DDL_TABLE;
        else {
			try {
				long tableNumber = Long.parseLong(tableName.getQualifierAsString());
				if (tableNumber < FIRST_USER_TABLE_NUMBER)
					return TableType.DERBY_SYS_TABLE;
			} catch(NumberFormatException nfe){
                return TableType.HBASE_TABLE;
            } catch (Exception e) {
                SpliceLogUtils.debug(LOG,tableName+" is not a number");
			}
			return TableType.USER_TABLE;
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
