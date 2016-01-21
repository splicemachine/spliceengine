package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.io.StorageFile;
import com.splicemachine.db.io.StorageRandomAccessFile;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @author Scott Fines
 *         Date: 1/21/16
 */
class MFile implements StorageFile{
    private final String directoryName;
    private final String fileName;
    private byte[] data;

    public MFile(String directoryName,String fileName){
        this.directoryName=directoryName;
        this.fileName=fileName;
    }

    @Override
    public String[] list(){
        return new String[]{fileName};
    }

    @Override
    public boolean canWrite(){
        return fileName!=null;
    }

    @Override
    public boolean exists(){
        return true;
    }

    @Override
    public boolean isDirectory(){
        return fileName==null;
    }

    @Override
    public boolean delete(){
        return true;
    }

    @Override
    public boolean deleteAll(){
        return true;
    }

    @Override
    public String getPath(){
        return directoryName+"/"+fileName;
    }

    @Override
    public String getCanonicalPath() throws IOException{
        return getPath();
    }

    @Override
    public String getName(){
        return fileName!=null? fileName:directoryName;
    }

    @Override
    public URL getURL() throws MalformedURLException{
        return null;
    }

    @Override
    public boolean createNewFile() throws IOException{
        return true;
    }

    @Override
    public boolean renameTo(StorageFile newName){
        return false;
    }

    @Override
    public boolean mkdir(){
        return true;
    }

    @Override
    public boolean mkdirs(){
        return true;
    }

    @Override
    public long length(){
        return data.length;
    }

    @Override
    public StorageFile getParentDir(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean setReadOnly(){
        return false;
    }

    @Override
    public OutputStream getOutputStream() throws FileNotFoundException{
        return getOutputStream(false);
    }

    @Override
    public OutputStream getOutputStream(boolean append) throws FileNotFoundException{
        return new ByteArrayOutputStream(){
            @Override
            public void close() throws IOException{
                MFile.this.data =super.toByteArray();
                super.close();
            }
        };
    }

    @Override
    public InputStream getInputStream() throws FileNotFoundException{
        return new ByteArrayInputStream(this.data);
    }

    @Override
    public int getExclusiveFileLock() throws StandardException{
        return 0;
    }

    @Override
    public void releaseExclusiveFileLock(){

    }

    @Override
    public StorageRandomAccessFile getRandomAccessFile(String mode) throws FileNotFoundException{
        return null;
    }

    @Override
    public void limitAccessToOwner(){

    }
}
