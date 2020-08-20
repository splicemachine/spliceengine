/*
 * Copyright 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.si.data.hbase.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.Lists;

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
    public void getWALPositions(RpcController controller,
                                   SpliceMessage.GetWALPositionsRequest request,
                                   RpcCallback<SpliceMessage.GetWALPositionsResponse> done) {

        SpliceMessage.GetWALPositionsResponse.Builder responseBuilder =
                SpliceMessage.GetWALPositionsResponse.newBuilder();

        try {
            List<WAL> wals = regionServerServices.getWALs();
            for (WAL wal : wals) {
                AbstractFSWAL abstractFSWAL = (AbstractFSWAL) wal;
                Path walName = abstractFSWAL.getCurrentFileName();
                OptionalLong size = wal.getLogFileSizeIfBeingWritten(walName);
                responseBuilder.addResult(
                        SpliceMessage.GetWALPositionsResponse.Result
                                .newBuilder()
                                .setPosition(size.isPresent() ? size.getAsLong() : 0)
                                .setWALName(walName.getName())
                                .build()
                );
            }

            SpliceMessage.GetWALPositionsResponse response = responseBuilder.build();
            done.run(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void getRegionServerLSN(RpcController controller,
                                   SpliceMessage.GetRegionServerLSNRequest request,
                                   RpcCallback<SpliceMessage.GetRegionServerLSNResponse> done) {

        SpliceMessage.GetRegionServerLSNResponse.Builder responseBuilder =
                SpliceMessage.GetRegionServerLSNResponse.newBuilder();


        List<? extends Region> regions = regionServerServices.getRegions();
        String walGroupId = request.hasWalGroupId() ? request.getWalGroupId() : null;
        try {

            for (Region region : regions) {
                HRegion hRegion = (HRegion) region;
                NavigableMap<byte[], java.lang.Integer> replicationScope = hRegion.getReplicationScope();
                if (region.isReadOnly() || replicationScope.isEmpty()){
                    // skip regions not enabled for replication
                    continue;
                }

                if (walGroupId != null) {
                    // skip regions for a different wal group
                    WAL wal = regionServerServices.getWAL(region.getRegionInfo());
                    if (wal.toString().indexOf(walGroupId) == -1) {
                        continue;
                    }
                }

                long readPoint = ((HRegion) region).getReadPoint(IsolationLevel.READ_COMMITTED);
                String encodedRegionName = region.getRegionInfo().getEncodedName();
                responseBuilder.addResult(
                        SpliceMessage.GetRegionServerLSNResponse.Result.
                                newBuilder().
                                setLsn(readPoint).
                                setRegionName(encodedRegionName).
                                setValid(true).build()
                );

            }
            SpliceMessage.GetRegionServerLSNResponse response = responseBuilder.build();
            done.run(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void getOldestActiveTransaction(RpcController controller,
                                           SpliceMessage.SpliceOldestActiveTransactionRequest request,
                                           RpcCallback<SpliceMessage.SpliceOldestActiveTransactionResponse> callback) {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getOldestActiveTransaction");
        SpliceMessage.SpliceOldestActiveTransactionResponse.Builder writeResponse = SpliceMessage.SpliceOldestActiveTransactionResponse.newBuilder();

        long oldestActiveTransaction = SIDriver.driver().getTxnStore().oldestActiveTransaction();
        writeResponse.setOldestActiveTransaction(oldestActiveTransaction);
        callback.run(writeResponse.build());
    }
}
