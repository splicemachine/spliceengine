package com.splicemachine.lifecycle;

import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.derby.lifecycle.DistributedDerbyStartup;
import com.splicemachine.hbase.ZkUtils;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class MasterLifecycle implements DistributedDerbyStartup{
    @Override
    public void distributedStart() throws IOException{
        try{
            if(ZkUtils.isSpliceLoaded()){
                HBaseConnectionFactory.createRestoreTableIfNecessary();
            }else{
                ZkUtils.refreshZookeeper();
                HBaseConnectionFactory.createSpliceHBaseTables();
            }
        }catch(InterruptedException e){
            throw new InterruptedIOException();
        }catch(KeeperException e){
            throw new IOException(e);
        }
    }

    @Override
    public void markBootFinished() throws IOException{
        try{
            ZkUtils.spliceFinishedLoading();
        }catch(InterruptedException e){
            throw new InterruptedIOException();
        }catch(KeeperException e){
            throw new IOException(e);
        }
    }

    @Override
    public boolean connectAsFirstTime(){
        return true;
    }
}
