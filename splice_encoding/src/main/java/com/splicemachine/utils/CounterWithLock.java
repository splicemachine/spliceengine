package com.splicemachine.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 
 *  This class allows to acquire write lock 
 *  only when counter == 0. It is the alternative 
 *  to ReentrantReadWriteLock as since it allows not 
 *  to keep read locks to prevent from getting write 
 *  lock on an object. This may be helpful when 
 *  acquiring/releasing read locks needs to be done
 *  from different threads (standard RRWL does not
 *  allow to do this) 
 *  
 */
public class CounterWithLock {
	
	
	private final ReentrantReadWriteLock lock;
	private AtomicInteger counter = new AtomicInteger(0);
	
	public CounterWithLock(){
		lock = new ReentrantReadWriteLock();
	}
	
	public CounterWithLock(boolean fair)
	{
		lock = new ReentrantReadWriteLock(fair);
	}
	
	public void increment()
	{
		lock.readLock().lock();
		counter.incrementAndGet();
		lock.readLock().unlock();
	}
	
	public boolean tryIncrement()
	{
		if( lock.readLock().tryLock() == false){
			return false;
		}
		counter.decrementAndGet();
		lock.readLock().unlock();
		return true;
	}
	
	public int counter()
	{
		return counter.get();
	}
	
	public void decrement()
	{
		lock.readLock().lock();
		counter.decrementAndGet();
		lock.readLock().unlock();
	}

	public boolean tryDecrement()
	{
		if( lock.readLock().tryLock() == false){
			return false;
		}
		counter.decrementAndGet();
		lock.readLock().unlock();
		return true;
	}
	
	public boolean tryLock()
	{
		lock.writeLock().lock();
		if( counter.get() == 0){
			// Do not release lock
			return true;
		} else{
			// Release write lock
			// as since counter != 0
			lock.writeLock().unlock();
			return false;
		}
	}
	
	public void unlock()
	{
		lock.writeLock().unlock();
	}
}
