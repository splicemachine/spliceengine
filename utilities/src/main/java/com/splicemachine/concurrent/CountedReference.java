package com.splicemachine.concurrent;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

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
