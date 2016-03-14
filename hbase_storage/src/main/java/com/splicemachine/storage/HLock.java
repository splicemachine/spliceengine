package com.splicemachine.storage;

import com.splicemachine.si.data.HExceptionFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.regionserver.HRegion;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author Scott Fines
 *         Date: 12/15/15
 */
public class HLock implements Lock{
    private final byte[] key;

    private HRegion.RowLock delegate;
    private HRegion region;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public HLock(HRegion region,byte[] key){
        this.key = key;
        this.region = region;
    }

    @Override public void lock(){
        try{
            delegate = region.getRowLock(key,true);
        }catch(IOException e){
            throw new RuntimeException(HExceptionFactory.INSTANCE.processRemoteException(e));
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException{
        lock();
    }

    @Override
    public boolean tryLock(){
        try{
            delegate = region.getRowLock(key,false); // Null Lock Delegate means not run...
            return delegate!=null;
        }catch(IOException e){
            throw new RuntimeException(HExceptionFactory.INSTANCE.processRemoteException(e));
        }
    }

    @Override
    public boolean tryLock(long time,@Nonnull TimeUnit unit) throws InterruptedException{
        try{
            delegate = region.getRowLock(key,false);
            return true;
        }catch(IOException e){
            throw new RuntimeException(HExceptionFactory.INSTANCE.processRemoteException(e));
        }
    }

    @Override
    public void unlock(){
        if(delegate!=null) delegate.release();
    }

    @Override
    public @Nonnull Condition newCondition(){
        throw new UnsupportedOperationException("Cannot support conditions on an HLock");
    }
}
