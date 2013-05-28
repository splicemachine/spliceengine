package com.splicemachine.tools;

/**
 * Represents a Poolable Resource.
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public interface ResourcePool<E,K extends ResourcePool.Key>{

    /**
     * Unique reference Key for the pool
     */
    public interface Key{

    }

    public interface Generator<T,K extends Key>{

        T makeNew(K refKey) throws Exception;

        void close(T entity);
    }

    public E get(K key) throws Exception;

    public void release(K key);

}
