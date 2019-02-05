/*
 * Copyright 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.hbase;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.replication.ReplicationMessage;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;

import org.apache.hadoop.hbase.wal.WAL;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class SpliceReplicationService extends ReplicationMessage.GetRegionServerLSNService
        implements Coprocessor, SingletonCoprocessorService
{
    private static final Logger LOG = Logger.getLogger(SpliceReplicationService.class);
    private RegionServerServices regionServerServices;
    private long testCount;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionServerCoprocessorEnvironment) {
            this.regionServerServices = ((RegionServerCoprocessorEnvironment) env).getRegionServerServices();
            this.testCount = 0;
            SpliceLogUtils.info(LOG,"Started SpliceReplicationService");
        } else {
            throw new CoprocessorException("Must be loaded on a RegionServer!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // nothing to do when coprocessor is shutting down
        SpliceLogUtils.info(LOG, "Shut down SpliceReplicationService");

    }

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void getRegionServerLSN(RpcController controller,
                                   ReplicationMessage.GetRegionServerLSNRequest request,
                                   RpcCallback<ReplicationMessage.GetRegionServerLSNReponse> done) {

        ReplicationMessage.GetRegionServerLSNReponse.Builder responseBuilder =
                ReplicationMessage.GetRegionServerLSNReponse.newBuilder();
        try {
            Set<Region> regionSet = new HashSet<Region>();
            // JY- TODO: only collect LSNs for tables under replication
            // Get all the online tables in this RS
            Set<TableName> tableSet = this.regionServerServices.getOnlineTables();
            for (TableName tableName : tableSet) {
                // get all the regions of this table on this RS
                regionSet.addAll(this.regionServerServices.getOnlineRegions(tableName));
            }

            // Go through each Region on this RS
            for (Region region : regionSet) {
                if (!region.isReadOnly()) {
                    // What should be the key value
                    WAL wal = regionServerServices.getWAL(region.getRegionInfo());
                    long readPoint = region.getReadpoint(IsolationLevel.READ_COMMITTED);
                    String encodedRegionName = region.getRegionInfo().getEncodedName();
                    responseBuilder.addResult(
                            ReplicationMessage.GetRegionServerLSNReponse.Result.
                                    newBuilder().
                                    setLsn(readPoint-1).
                                    setRegionName(encodedRegionName).
                                    setValid(true).build()
                    );
                }
            }
            ReplicationMessage.GetRegionServerLSNReponse response = responseBuilder.build();
            done.run(response);
        }
        catch (IOException ioe) {
            SpliceLogUtils.error(LOG, ioe);
            // Call ServerRpcController#getFailedOn() to retrieve this IOException at client side.
            ResponseConverter.setControllerException(controller, ioe);
        }
    }
}
