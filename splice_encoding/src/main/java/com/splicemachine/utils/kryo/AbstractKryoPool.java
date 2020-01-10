/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.utils.kryo;

import com.esotericsoftware.kryo.Kryo;
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
