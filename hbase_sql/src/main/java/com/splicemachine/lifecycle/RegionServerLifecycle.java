package com.splicemachine.lifecycle;

import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.SpliceMasterObserver;
import com.splicemachine.derby.lifecycle.DistributedDerbyStartup;
import com.splicemachine.hbase.SpliceMetrics;
import com.splicemachine.hbase.ZkUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class RegionServerLifecycle implements DistributedDerbyStartup{
    private final Clock clock;

    public RegionServerLifecycle(Clock clock){
        this.clock=clock;
    }

    @Override
    public void distributedStart() throws IOException{
        Connection conn =HBaseConnectionFactory.getInstance().getConnection();
        boolean onHold;
        try(Admin admin=conn.getAdmin()){
            do{
                onHold=false;
                try{
                    HTableDescriptor desc=new HTableDescriptor(TableName.valueOf(SpliceMasterObserver.INIT_TABLE));
                    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("FOO")));
                    // Create the special "SPLICE_INIT" table which triggers the creation of the SpliceMasterObserver and ultimately
                    // triggers the creation of the "SPLICE_*" HBase tables.  This is an asynchronous call and so we "loop" via a
                    // "please hold" exception and a recursive call to bootDatabase() along with a sleep.
                    admin.createTable(desc);
                }catch(PleaseHoldException pe){
                    onHold=true;
                    try{
                        clock.sleep(1,TimeUnit.SECONDS);
                    }catch(InterruptedException e){
                        //startup was interrupted, so throw an InterruptedIOException
                        throw new InterruptedIOException();
                    }
                }catch(DoNotRetryIOException dnre){
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
                    SpliceConstants.upgradeForced = false;

                    // Ensure ZK paths exist.
                    try{
                        ZkUtils.safeInitializeZooKeeper();
                    }catch(InterruptedException e){
                        throw new InterruptedIOException();
                    }catch(KeeperException e){
                        throw new IOException(e);
                    }
                }
            }while(onHold);
        }
        //register splice metrics
        new SpliceMetrics();
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
