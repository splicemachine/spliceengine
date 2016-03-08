package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.db.io.StorageFile;

import java.io.File;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MemStorageFactory implements StorageFactory{
    private String canonicalName;

    /**
     * Most of the initialization is done in the init method.
     */
    public MemStorageFactory(){
        super();
    }

    @Override
    public void init(String home,String databaseName,String tempDirName,
                     String uniqueName) throws IOException{
        doInit();
    }

    void doInit() throws IOException{
    } // end of doInit

    @Override
    public void shutdown(){
    }

    @Override
    public String getCanonicalName() throws IOException{
        return canonicalName;
    }

    @Override
    public StorageFile newStorageFile(String path){
        return new MFile(path,null);
    }

    @Override
    public StorageFile newStorageFile(String directoryName,String fileName){
        return new MFile(directoryName,fileName);
    }

    @Override
    public StorageFile newStorageFile(StorageFile directoryName,String fileName){
        return newStorageFile(directoryName.getPath(),fileName);
    }

    @Override
    public char getSeparator(){
        return File.separatorChar;
    }

    @Override
    public StorageFile getTempDir(){
        return null;
    }

    @Override
    public boolean isFast(){
        return false;
    }

    @Override
    public boolean isReadOnlyDatabase(){
        return false;
    }

    @Override
    public boolean supportsRandomAccess(){
        return false;
    }

    @Override
    public int getStorageFactoryVersion(){
        return StorageFactory.VERSION_NUMBER;
    }

    @Override
    public StorageFile createTemporaryFile(String prefix,String suffix)
            throws IOException{
        return null;
    }

    @Override
    public void setCanonicalName(String name){
        canonicalName=name;
    }

}

