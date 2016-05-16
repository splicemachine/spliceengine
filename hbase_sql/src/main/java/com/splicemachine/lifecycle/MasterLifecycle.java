package com.splicemachine.lifecycle;

import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.derby.lifecycle.DistributedDerbyStartup;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class MasterLifecycle implements DistributedDerbyStartup{
    private boolean loading = false;
    @Override
    public void distributedStart() throws IOException{
        try{
            HBaseConnectionFactory instance=HBaseConnectionFactory.getInstance(SIDriver.driver().getConfiguration());
            if(!ZkUtils.isSpliceLoaded()){
                loading = true;
                ZkUtils.refreshZookeeper();
                instance.createSpliceHBaseTables();
            }
        }catch(InterruptedException e){
            throw new InterruptedIOException();
        }catch(KeeperException e){
            throw new IOException(e);
        }
    }

    @Override
    public void markBootFinished() throws IOException{
        if(loading){
            try{
                ZkUtils.spliceFinishedLoading();
            }catch(InterruptedException e){
                throw new InterruptedIOException();
            }catch(KeeperException e){
                throw new IOException(e);
            }
        }
    }

    @Override
    public boolean connectAsFirstTime(){
        return loading;
    }
}
