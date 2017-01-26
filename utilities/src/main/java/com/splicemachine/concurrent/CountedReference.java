/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.concurrent;

import org.spark_project.guava.base.Supplier;
import org.spark_project.guava.base.Suppliers;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility class for maintaining a thread-safe count of references
 * to a given object.
 *
 * This is useful when constructing a thread-safe object which is
 * expensive to maintain (e.g. has internal thread pools or something similar)
 * in a clean, maintainable way.
 *
 * @author Scott Fines
 * Date: 10/6/14
 */
public class CountedReference<T> {
    private final Supplier<T> generator;
    private final ShutdownAction<T> shutdownAction;
    private volatile T reference;
    private AtomicInteger referenceCount = new AtomicInteger(0);

    public interface ShutdownAction<T>{
        void shutdown(T instance);
    }

    private static final ShutdownAction NO_OP_ACTION = new ShutdownAction() {
        @Override public void shutdown(Object instance) {  }
    };

    @SuppressWarnings("unchecked")
    public static <T> ShutdownAction<T> noOpShutown(){
        return (ShutdownAction<T>)NO_OP_ACTION;
    }

    public CountedReference(Supplier<T> generator,
                            ShutdownAction<T> shutdownAction) {
        this.generator = generator;
        this.shutdownAction = shutdownAction;
    }

    @SuppressWarnings("unchecked")
    public static <T> CountedReference<T> wrap(T instance){
        return new CountedReference<T>(Suppliers.ofInstance(instance),
                (ShutdownAction<T>)NO_OP_ACTION);
    }

    public T get(){
        referenceCount.incrementAndGet();
        return getReference();
    }

    public void release(boolean shutdown){
        int remaining = referenceCount.decrementAndGet();
        if(remaining<=0 && shutdown){
            shutdownAction.shutdown(reference);
            reference = null; //release for garbage collection
        }
    }

    private T getReference() {
        T ref = reference;
        if(ref==null){
            synchronized (this){
                ref = reference;
                if(ref==null){
                    ref = reference = generator.get();
                }
            }
        }
        return ref;
    }

}
