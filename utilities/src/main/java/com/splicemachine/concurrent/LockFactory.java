package com.splicemachine.concurrent;

import java.util.concurrent.locks.Lock;

/**
 * A Factory for constructing locks. This is useful when you want to replace different lock implementations
 * (i.e. for simulating lock timeout etc. without relying on the system clock).
 *
 * @author Scott Fines
 *         Date: 9/4/15
 */
public interface LockFactory{

    /**
     * @return a new lock from the factory
     */
    Lock newLock();
}
