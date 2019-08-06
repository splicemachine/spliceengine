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
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Lists;

import java.io.IOException;
import java.util.*;

public class SpliceRSRpcServices extends SpliceMessage.SpliceRSRpcServices implements RegionServerCoprocessor {

    private static final Logger LOG = Logger.getLogger(SpliceRSRpcServices.class);
    private RegionServerServices regionServerServices;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionServerCoprocessorEnvironment) {
            this.regionServerServices = (RegionServerServices) ((RegionServerCoprocessorEnvironment) env).getOnlineRegions();
            SpliceLogUtils.info(LOG,"Started SpliceRSRpcServices");
        } else {
            throw new CoprocessorException("Must be loaded on a RegionServer!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // nothing to do when coprocessor is shutting down
        SpliceLogUtils.info(LOG, "Shut down SpliceRSRpcServices");

    }

    @Override
    public Iterable<Service> getServices() {
        List<Service> services = Lists.newArrayList();
        services.add(this);
        return services;
    }

    @Override
    public void getRegionServerLSN(RpcController controller,
                                   SpliceMessage.GetRegionServerLSNRequest request,
                                   RpcCallback<SpliceMessage.GetRegionServerLSNReponse> done) {

        SpliceMessage.GetRegionServerLSNReponse.Builder responseBuilder =
                SpliceMessage.GetRegionServerLSNReponse.newBuilder();

        List<? extends Region> regions = regionServerServices.getRegions();
        for (Region region : regions) {
            HRegion hRegion = (HRegion)region;
            NavigableMap<byte[],java.lang.Integer> replicationScope = hRegion.getReplicationScope();
            if (!region.isReadOnly() && !replicationScope.isEmpty()) {
                long readPoint = ((HRegion)region).getReadPoint(IsolationLevel.READ_COMMITTED);
                String encodedRegionName = region.getRegionInfo().getEncodedName();
                responseBuilder.addResult(
                        SpliceMessage.GetRegionServerLSNReponse.Result.
                                newBuilder().
                                setLsn(readPoint).
                                setRegionName(encodedRegionName).
                                setValid(true).build()
                );
            }
        }
        SpliceMessage.GetRegionServerLSNReponse response = responseBuilder.build();
        done.run(response);
    }
}
