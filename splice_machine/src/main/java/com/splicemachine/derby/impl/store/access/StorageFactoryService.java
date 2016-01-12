package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.derby.stream.iapi.IterableJoinFunction;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class StorageFactoryService{
    public static StorageFactory newStorageFactory(){
        ServiceLoader<StorageFactory> factoryService = ServiceLoader.load(StorageFactory.class);
        Iterator<StorageFactory> iter = factoryService.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No StorageFactory service found!");
        StorageFactory sf = iter.next();
        if(iter.hasNext())
            throw new IllegalStateException("Can only have one StorageFactory service!");
        return sf;
    }
}
