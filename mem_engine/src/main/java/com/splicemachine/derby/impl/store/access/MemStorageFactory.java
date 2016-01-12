package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.db.io.StorageFile;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MemStorageFactory implements StorageFactory{
    String home;
    protected String dataDirectory;
    protected String separatedDataDirectory; // dataDirectory + separator
    protected String uniqueName;
    protected String canonicalName;

    /**
     * Most of the initialization is done in the init method.
     */
    public MemStorageFactory(){
        super();
    }

    @Override
    public void init(String home,String databaseName,String tempDirName,
                     String uniqueName) throws IOException{
        if(databaseName!=null){
            dataDirectory=databaseName;
            separatedDataDirectory=databaseName+getSeparator();
        }
        this.home=home;
        this.uniqueName=uniqueName;
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
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public StorageFile newStorageFile(String directoryName,String fileName){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public StorageFile newStorageFile(StorageFile directoryName,String fileName){
        throw new UnsupportedOperationException("IMPLEMENT");
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

//    @Override
    public void sync(OutputStream stream,boolean metaData) throws IOException{
        stream.flush();
    }

//    @Override
    public boolean supportsWriteSync(){
        return false;
    }
}

