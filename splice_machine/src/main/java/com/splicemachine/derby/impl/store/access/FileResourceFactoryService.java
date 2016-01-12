package com.splicemachine.derby.impl.store.access;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class FileResourceFactoryService{
    public static FileResourceFactory loadFileResourceFactory(){
        ServiceLoader<FileResourceFactory> loader = ServiceLoader.load(FileResourceFactory.class);
        Iterator<FileResourceFactory> iter = loader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No FileResourceFactory found!");
        FileResourceFactory frf = iter.next();
        if(iter.hasNext())
            throw new IllegalStateException("More than one FileResourceFactory is found!");
        return frf;
    }
}
