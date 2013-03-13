package com.splicemachine.tools;

/**
 * Represents a Poolable Resource.
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public interface ResourcePool<E>{

    /**
     * Unique reference Key for the pool
     */
    public interface Key{

    }

    public interface Generator<T>{

        T makeNew(Key refKey);

        void close(T entity);
    }

    public E get(Key key);

    public void release(Key key);

}
