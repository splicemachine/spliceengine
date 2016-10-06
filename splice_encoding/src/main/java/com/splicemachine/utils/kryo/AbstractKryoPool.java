/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Simple Pool of Kryo objects that allows a core of Kryo objects to remain
 * available for re-use, while requiring only a single thread access to a Kryo instance
 * at a time. If the pool is exhausted, then this will create new Kryo objects.
 *
 * It is abstract so we can re-use the structure for a custom Spark Serializer
 *
 * @author Scott Fines
 * Created on: 8/15/13
 */
public abstract class AbstractKryoPool {
    protected final Queue<Kryo> instances;
    protected volatile KryoRegistry kryoRegistry;
    protected int poolSize;

    public AbstractKryoPool(int poolSize) {
        this.poolSize = poolSize;
        this.instances =new ConcurrentLinkedQueue<>();
    }

    public void setKryoRegistry(KryoRegistry kryoRegistry){
        this.kryoRegistry = kryoRegistry;
    }

    public Kryo get(){
        //try getting an instance that already exists
        Kryo next = instances.poll();
        if(next==null){
            next = newInstance();
        }
        return next;
    }

    public abstract Kryo newInstance();

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",justification = "Intentional")
    public void returnInstance(Kryo kryo){
        /*
         * If the pool is full, then we will allow kryo to run out of scope,
         * which will allow the GC to collect it. Thus, we can suppress
         * the findbugs warning
         */
        if(instances.size()< this.poolSize){
            instances.offer(kryo);
        }

    }
    public interface KryoRegistry{
        void register(Kryo instance);
    }
}
