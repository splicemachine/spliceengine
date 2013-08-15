package com.splicemachine.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.splicemachine.constants.SpliceConstants;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Simple Pool of Kryo objects that allows a core of Kryo objects to remain
 * available for re-use, while requiring only a single thread access to a Kryo instance
 * at a time. If the pool is exhausted, then this will create new Kryo objects.
 *
 * @author Scott Fines
 * Created on: 8/15/13
 */
public class KryoPool {
    private final BlockingQueue<Kryo> instances;
    private static final KryoPool DEFAULT_POOL = new KryoPool(SpliceConstants.kryoPoolSize);

    private volatile KryoRegistry kryoRegistry;

    public KryoPool(int poolSize) {
        this.instances = new ArrayBlockingQueue<Kryo>(poolSize);
    }

    public void setKryoRegistry(KryoRegistry kryoRegistry){
        this.kryoRegistry = kryoRegistry;
    }

    public Kryo get(){
        //try getting an instance that already exists
        Kryo next = instances.poll();
        if(next==null){
            next = new Kryo();
            if(kryoRegistry!=null)
                kryoRegistry.register(next);
        }

        return next;
    }

    public void returnInstance(Kryo kryo){
        /*
         * If the pool is full, then we will allow kryo to run out of scope,
         * which will allow the GC to collect it.
         */
        instances.offer(kryo);
    }

    public static KryoPool defaultPool() {
        return DEFAULT_POOL;
    }

    public static interface KryoRegistry{
        public void register(Kryo instance);
    }
}
