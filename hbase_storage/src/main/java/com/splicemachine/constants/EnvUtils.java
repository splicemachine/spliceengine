/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.constants;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryImpl;
import com.splicemachine.si.data.hbase.coprocessor.TableType;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

public class EnvUtils {
	private static Logger LOG = Logger.getLogger(EnvUtils.class);

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
				if (tableNumber < DataDictionaryImpl.FIRST_USER_TABLE_NUMBER)
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
