package com.splicemachine.storage;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author Scott Fines
 *         Date: 12/15/15
 */
public class HLock implements Lock{
    private HRegion.RowLock delegate;
    private HRegion region;
    private final byte[] key;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public HLock(HRegion region,byte[] key){
        this.key = key;
        this.region = region;
    }

    @Override public void lock(){
        try{
            delegate = region.getRowLock(key,true);
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException{
        lock();
    }

    @Override
    public boolean tryLock(){
        try{
            delegate = region.getRowLock(key,false);
            return true;
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean tryLock(long time,TimeUnit unit) throws InterruptedException{
        try{
            delegate = region.getRowLock(key,false);
            return true;
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unlock(){
        if(delegate!=null) delegate.release();
    }

    @Override
    public Condition newCondition(){
        throw new UnsupportedOperationException("Cannot support conditions on an HLock");
    }
}
