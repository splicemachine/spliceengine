/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.lifecycle;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ServiceException;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.pipeline.InitializationCompleted;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionOfflineException;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.lifecycle.DistributedDerbyStartup;
import com.splicemachine.hbase.SpliceMasterObserver;
import com.splicemachine.hbase.SpliceMetrics;
import com.splicemachine.hbase.ZkUtils;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class RegionServerLifecycle implements DistributedDerbyStartup{
    private static final Logger LOG = Logger.getLogger(RegionServerLifecycle.class);
    
    private final Clock clock;
    private final HBaseConnectionFactory connectionFactory;

    public RegionServerLifecycle(Clock clock,HBaseConnectionFactory connectionFactory){
        this.clock=clock;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void distributedStart() throws IOException{
        Connection conn =connectionFactory.getConnection();
        boolean onHold;
        try(Admin admin=conn.getAdmin()){
            do{
                onHold=false;
                try{
                    try {
                        SpliceMessage.SpliceInitRequest request = SpliceMessage.SpliceInitRequest.newBuilder().build();
                        SpliceMessage.SpliceInitResponse response = SpliceMessage.SpliceInitResponse.newBuilder().buildPartial();
                        admin.coprocessorService()
                                .callBlockingMethod(
                                        SpliceMessage.SpliceMasterCoprocessorService.getDescriptor().findMethodByName("spliceInit"),
                                        RpcControllerFactory.instantiate(conn.getConfiguration()).newController(), request, response);
                    } catch (ServiceException se) {
                        throw se.getCause();
                    }
                    throw new ServiceException("Shouldn't have received a response");
                }catch(InitializationCompleted ic) {
                    /*
                     * The exception signaling the start of a successfully running Splice Engine will be a SpliceDoNotRetryIOException.
                     * When this is returned, it means that a connection to HBase and the creation (or previous existence) of the
                     * "SPLICE_*" tables in HBase has been successful.
                     */

                    /*
                     * If an upgrade has been forced, we are now finished with it since this bit of code here only runs
                     * on the region servers and we don't ever run upgrade code on region servers.  Only the master server
                     * runs upgrade code via the SpliceMasterObserver.  So we mark the flag to false to ensure that the
                     * region servers don't run the upgrade.
                     */
                    SQLConfiguration.upgradeForced = false;

                    // Ensure ZK paths exist.
                    try {
                        ZkUtils.safeInitializeZooKeeper();
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException();
                    } catch (KeeperException e) {
                        throw new IOException(e);
                    }
                } catch(Throwable t){
                    if (!causeIsPleaseHold(t)) {
                        if (t instanceof IOException)
                            throw (IOException) t;
                        else {
                            LOG.error("Unexpected throwable", t);
                            throw new IOException(t);
                        }
                    }
                    onHold = true;
                    try {
                        clock.sleep(1, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        //startup was interrupted, so throw an InterruptedIOException
                        throw new InterruptedIOException();
                    }
                }
            }while(onHold);
        }
        //register splice metrics
        new SpliceMetrics();
    }

    private boolean causeIsPleaseHold(Throwable e) {
        if (e instanceof PleaseHoldException)
            return true;
        if (e instanceof TableNotEnabledException)
            return true;
        if (e instanceof RegionOfflineException)
            return true;
        if (e instanceof MasterNotRunningException)
            return true;
        if (e instanceof RetriesExhaustedException || e instanceof SocketTimeoutException) {
            if (e.getCause() instanceof RemoteException) {
                RemoteException re = (RemoteException) e.getCause();
                if (PleaseHoldException.class.getName().equals(re.getClassName()))
                    return true;
            }
        }
        return false;
    }

    @Override
    public void markBootFinished() throws IOException{
        //no-op on region servers
    }

    @Override
    public boolean connectAsFirstTime(){
        return false;
    }
}
