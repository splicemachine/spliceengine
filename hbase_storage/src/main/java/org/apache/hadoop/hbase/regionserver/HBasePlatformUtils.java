/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package org.apache.hadoop.hbase.regionserver;


import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HBasePlatformUtils{
    private static final Logger LOG = Logger.getLogger(HBasePlatformUtils.class);


    public static boolean scannerEndReached(ScannerContext scannerContext) {
        scannerContext.setSizeLimitScope(ScannerContext.LimitScope.BETWEEN_ROWS);
        scannerContext.incrementBatchProgress(1);
        scannerContext.incrementSizeProgress(100l, 100l);
        return scannerContext.setScannerState(ScannerContext.NextState.BATCH_LIMIT_REACHED).hasMoreValues();
    }

    public static void bulkLoad(Configuration conf, LoadIncrementalHFiles loader,
                                Path path, String fullTableName) throws IOException {
        SConfiguration configuration = HConfiguration.getConfiguration();
        org.apache.hadoop.hbase.client.Connection conn = HBaseConnectionFactory.getInstance(configuration).getConnection();
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        TableName tableName = TableName.valueOf(fullTableName);
        RegionLocator locator = conn.getRegionLocator(tableName);
        Table table = conn.getTable(tableName);
        loader.doBulkLoad(path, admin, table, locator);
    }
}
