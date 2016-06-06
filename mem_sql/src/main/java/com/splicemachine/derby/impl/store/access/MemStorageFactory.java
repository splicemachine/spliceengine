package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.db.io.StorageFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MemStorageFactory implements StorageFactory{
    private String canonicalName;
    private final ConcurrentMap<String,MFile> fileMap= new ConcurrentHashMap<>();

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

    List<Path> getChildren(Path path){
        List<Path> children = new LinkedList<>();
        Collection<MFile> values=fileMap.values();
        for(MFile sf:values){
            if(path.equals(sf.path().getParent()))
                children.add(sf.path());
        }
        return children;
    }

    void remove(MFile mFile){
        fileMap.remove(mFile.path().toString());
    }

    private void doInit() throws IOException{
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
        StorageFile f = fileMap.get(path);
        if(f==null){
            boolean isDir = path.endsWith("/");
            MFile newFile = new MFile(this,Paths.get(path),isDir);
            f =fileMap.putIfAbsent(path,newFile);
            if(f==null)
                f = newFile;
        }
        return f;
    }

    @Override
    public StorageFile newStorageFile(String directoryName,String fileName){
        if(fileName==null)
            return newStorageFile(directoryName);
        else
            return newStorageFile(directoryName+"/"+fileName);
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

