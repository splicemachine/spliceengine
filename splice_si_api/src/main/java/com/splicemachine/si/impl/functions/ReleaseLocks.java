package com.splicemachine.si.impl.functions;

import org.spark_project.guava.base.Function;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.Lock;

/**
 *
 * Release Locks
 *
 */
public class ReleaseLocks implements Function<Lock[],Collection> {

    public ReleaseLocks() {
    }

    @Nullable
    @Override
    public Collection apply(Lock[] locks) {
        if(locks==null)
            return Collections.emptyList();
        for(Lock lock : locks){
            if(lock==null)
                continue;
            lock.unlock();
        }
        return Collections.emptyList();
    }
}
