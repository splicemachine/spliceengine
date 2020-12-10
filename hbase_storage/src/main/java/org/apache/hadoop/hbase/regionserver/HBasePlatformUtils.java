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
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.SkeletonHBaseClientPartition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Throwables;
import splice.com.google.common.collect.Sets;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public static Set<String> getCompactedFilesPathsFromHBaseRegionServer(RegionInfo hri) {
        try {
            String regionName = hri.getRegionNameAsString();
            try (Partition partition = SIDriver.driver().getTableFactory().getTable(hri.getTable())) {
                Map<byte[], List<String>> results = ((SkeletonHBaseClientPartition) partition).coprocessorExec(
                        SpliceMessage.SpliceDerbyCoprocessorService.class,
                        hri.getStartKey(),
                        hri.getStartKey(),
                        instance -> {
                            ServerRpcController controller = new ServerRpcController();
                            SpliceMessage.GetCompactedHFilesRequest message = SpliceMessage.GetCompactedHFilesRequest
                                    .newBuilder()
                                    .setRegionEncodedName(regionName)
                                    .build();

                            CoprocessorRpcUtils.BlockingRpcCallback<SpliceMessage.GetCompactedHFilesResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<>();
                            instance.getCompactedHFiles(controller, message, rpcCallback);
                            if (controller.failed()) {
                                Throwable t = Throwables.getRootCause(controller.getFailedOn());
                                if (t instanceof IOException) throw (IOException) t;
                                else throw new IOException(t);
                            }
                            SpliceMessage.GetCompactedHFilesResponse response = rpcCallback.get();
                            return response.getFilePathList();
                        });
                //assert results.size() == 1: results;
                return Sets.newHashSet(results.get(hri.getRegionName()));
            }
        } catch (Throwable e) {
            SpliceLogUtils.error(LOG, "Unable to set Compacted Files from HBase region server", e);
            throw new RuntimeException(e);
        }
    }
}
