package com.splicemachine.concurrent;

import java.util.concurrent.locks.Lock;

/**
 * @author Scott Fines
 *         Date: 9/4/15
 */
public class SingleInstanceLockFactory implements LockFactory{

    private final Lock lock;

    public SingleInstanceLockFactory(Lock lock){
        this.lock=lock;
    }

    @Override public Lock newLock(){ return lock; }
}
