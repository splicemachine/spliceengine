package com.splicemachine.concurrent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Scott Fines
 *         Date: 9/4/15
 */
public class ReentrantLockFactory implements LockFactory{
    private static final ReentrantLockFactory INSTANCE = new ReentrantLockFactory(false);
    private static final ReentrantLockFactory FAIR_INSTANCE = new ReentrantLockFactory(true);

    private final boolean fair;

    public ReentrantLockFactory(boolean fair){
        this.fair=fair;
    }

    @Override
    public Lock newLock(){
        return new ReentrantLock(fair);
    }

    public static LockFactory instance(){ return INSTANCE;}

    public static LockFactory fairInstance(){ return FAIR_INSTANCE;}

}
