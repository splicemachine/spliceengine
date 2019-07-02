/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.pipeline.InitializationCompleted;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionOfflineException;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
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
                TableName spliceInit = TableName.valueOf(SpliceMasterObserver.INIT_TABLE);
                try{
                    HTableDescriptor desc=new HTableDescriptor(spliceInit);
                    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("FOO")));
                    // Create the special "SPLICE_INIT" table which triggers the creation of the SpliceMasterObserver and ultimately
                    // triggers the creation of the "SPLICE_*" HBase tables.  This is an asynchronous call and so we "loop" via a
                    // "please hold" exception and a recursive call to bootDatabase() along with a sleep.
                    admin.createTable(desc);
                    LOG.error("We could create SPLICE_INIT table, this means the SpliceMasterObserver hasn't loaded properly");
                    try {
                        admin.disableTable(spliceInit);
                        admin.deleteTable(spliceInit);
                    } catch (IOException e) {
                        LOG.warn("Received exception while cleaning up SPLICE_INIT", e);
                    }
                    throw new DoNotRetryIOException("We could create SPLICE_INIT table, SpliceMasterObser not loaded");
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
                } catch (TableExistsException e) {
                    LOG.error("SPLICE_INIT already exists, this shouldn't happen. Remove it and retry");
                    try {
                        admin.disableTable(spliceInit);
                        admin.deleteTable(spliceInit);
                    } catch (IOException ioe) {
                        LOG.warn("Received exception while cleaning up SPLICE_INIT", e);
                    }
                    onHold = true;
                } catch(Exception ex){
                    if (!causeIsPleaseHold(ex))
                        throw ex;
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
        if (e instanceof RetriesExhaustedException || e instanceof SocketTimeoutException) {
            if (e.getCause() instanceof RemoteException) {
                RemoteException re = (RemoteException) e.getCause();
                if (PleaseHoldException.class.getName().equals(re.getClassName()))
                    return true;
            } else if (e.getCause() instanceof MasterNotRunningException) {
                return true;
            }
            return (e.getCause() instanceof IOException && e.getCause().getCause() instanceof CallTimeoutException) ||
                   (e.getCause() instanceof RemoteWithExtrasException && e.getMessage().equals(
                           "Table Namespace Manager not fully initialized, try again later"));
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
