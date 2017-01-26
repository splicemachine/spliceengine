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

package com.splicemachine.storage;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.util.Map;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 1/21/16
 */
public class HNIOFileSystem extends DistributedFileSystem{
    private final org.apache.hadoop.fs.FileSystem fs;
    private final boolean isDistributedFS;
    private final ExceptionFactory exceptionFactory;
    private static Logger LOG=Logger.getLogger(HNIOFileSystem.class);

    public HNIOFileSystem(org.apache.hadoop.fs.FileSystem fs,ExceptionFactory ef){
        this.fs=fs;
        this.exceptionFactory = ef;
        this.isDistributedFS = (fs instanceof org.apache.hadoop.hdfs.DistributedFileSystem);
    }

    @Override
    public void delete(Path path) throws IOException{
        delete(path,false);
    }

    @Override
    public void delete(Path path,boolean recursive) throws IOException{
        fs.delete(toHPath(path),recursive);
    }

    @Override
    public void delete(String dir,boolean recursive) throws IOException{
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "delete(): dir=%s, recursive=%s", dir, recursive);
        org.apache.hadoop.fs.Path p=new org.apache.hadoop.fs.Path(dir);
        boolean result = fs.delete(p,recursive);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "delete(): dir=%s, recursive=%s, result=%s", dir, recursive, result);
    }

    @Override
    public void delete(String dir,String fileName,boolean recursive) throws IOException{
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "delete(): dir=%s, fileName=%s, recursive=%s", dir, fileName, recursive);
        org.apache.hadoop.fs.Path p=new org.apache.hadoop.fs.Path(dir,fileName);
        boolean result = fs.delete(p,recursive);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "delete(): dir=%s, fileName=%s, recursive=%s, result=%s", dir, fileName, recursive, result);
    }

    public String[] getExistingFiles(String dir, String filePattern) throws IOException {
        FileStatus[] statuses = fs.globStatus(new org.apache.hadoop.fs.Path(dir, filePattern));
        String[] files = new String[statuses.length];
        int index = 0;
        for (FileStatus status : statuses) {
            if (status != null && status.getPath() != null)
                files[index++] = status.getPath().getName();
        }
        return files;
    }

    @Override
    public Path getPath(String directory,String fileName){
        return Paths.get(directory,fileName);
    }

    @Override
    public Path getPath(String fullPath){
        return Paths.get(fullPath);
    }

    @Override
    public FileInfo getInfo(String filePath) throws IOException{
        org.apache.hadoop.fs.Path f=new org.apache.hadoop.fs.Path(filePath);
        ContentSummary contentSummary;
        try{
            contentSummary=fs.getContentSummary(f);
        }catch(IOException ioe){
            LOG.error("Unexpected error getting content summary. We ignore it for now, but you should probably check it out:",ioe);
            contentSummary = new ContentSummary(0L,0L,0L);

        }
        return new HFileInfo(f,contentSummary);
    }

    @Override
    public String getScheme(){
        return fs.getScheme();
    }

    @Override
    public FileSystem newFileSystem(URI uri,Map<String, ?> env) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public FileSystem getFileSystem(URI uri){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Path getPath(URI uri){
        return Paths.get(uri);
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path,Set<? extends OpenOption> options,FileAttribute<?>... attrs) throws IOException{
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir,DirectoryStream.Filter<? super Path> filter) throws IOException{
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputStream newOutputStream(Path path,OpenOption... options) throws IOException{
        return fs.create(toHPath(path));
    }

    @Override
    public OutputStream newOutputStream(String dir,String fileName,OpenOption... options) throws IOException{
        org.apache.hadoop.fs.Path path=new org.apache.hadoop.fs.Path(dir,fileName);
        return fs.create(path);
    }

    @Override
    public OutputStream newOutputStream(String fullPath,OpenOption... options) throws IOException{
        org.apache.hadoop.fs.Path path=new org.apache.hadoop.fs.Path(fullPath);
        return fs.create(path);
    }

    @Override
    public void createDirectory(Path dir,FileAttribute<?>... attrs) throws IOException{
        org.apache.hadoop.fs.Path f=toHPath(dir);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "createDirectory(): path=%s", f);
        try{
            FileStatus fileStatus=fs.getFileStatus(f);
            throw new FileAlreadyExistsException(dir.toString());
        }catch(FileNotFoundException fnfe){
            fs.mkdirs(f);
        }
    }

    @Override
    public boolean createDirectory(Path path,boolean errorIfExists) throws IOException{
        org.apache.hadoop.fs.Path f=toHPath(path);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "createDirectory(): path=%s", f);
        try{
            FileStatus fileStatus=fs.getFileStatus(f);
            return !errorIfExists && fileStatus.isDirectory();
        }catch(FileNotFoundException fnfe){
            return fs.mkdirs(f);
        }
    }

    @Override
    public boolean createDirectory(String fullPath,boolean errorIfExists) throws IOException{
        boolean isTrace = LOG.isTraceEnabled();
        if (isTrace)
            SpliceLogUtils.trace(LOG, "createDirectory(): path string=%s", fullPath);
        org.apache.hadoop.fs.Path f=new org.apache.hadoop.fs.Path(fullPath);
        if (isTrace)
            SpliceLogUtils.trace(LOG, "createDirectory(): hdfs path=%s", f);
        try{
            FileStatus fileStatus=fs.getFileStatus(f);
            if (isTrace)
                SpliceLogUtils.trace(LOG, "createDirectory(): file status=%s", fileStatus);
            return !errorIfExists && fileStatus.isDirectory();
        }catch(FileNotFoundException fnfe){
            if (isTrace)
                SpliceLogUtils.trace(LOG, "createDirectory(): directory not found so we will create it: %s", f);
            boolean created = fs.mkdirs(f);
            if (isTrace)
                SpliceLogUtils.trace(LOG, "createDirectory(): created=%s", created);
            return created;
        }
    }

    @Override
    public void touchFile(Path path) throws IOException{
        if(!fs.createNewFile(toHPath(path))){
            throw new FileAlreadyExistsException(path.toString());
        }
    }

    @Override
    public void touchFile(String dir, String fileName) throws IOException{
        org.apache.hadoop.fs.Path path=new org.apache.hadoop.fs.Path(dir,fileName);
        if(!fs.createNewFile(path)){
            throw new FileAlreadyExistsException(path.toString());
        }
    }

    @Override
    public void concat(Path target, Path... sources)  throws IOException {
        org.apache.hadoop.fs.Path[] srcPaths = new org.apache.hadoop.fs.Path[sources.length];
        for (int i=0; i<sources.length; i++) {
            srcPaths[i] = new org.apache.hadoop.fs.Path(sources[i].getParent().toString(), sources[i].getFileName().toString());
        }
        org.apache.hadoop.fs.Path targetPath = new org.apache.hadoop.fs.Path(target.getParent().toString(), target.getFileName().toString());


        if (isDistributedFS) {
            fs.concat(targetPath, srcPaths);
        } else {
            for (org.apache.hadoop.fs.Path src : srcPaths) {
                fs.copyFromLocalFile(true, false, src, targetPath);
            }
        }
    }

    @Override
    public void copy(Path source,Path target,CopyOption... options) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void move(Path source,Path target,CopyOption... options) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean isSameFile(Path path,Path path2) throws IOException{
        return path.equals(path2);
    }

    @Override
    public boolean isHidden(Path path) throws IOException{
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void checkAccess(Path path,AccessMode... modes) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path,Class<V> type,LinkOption... options){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path,Class<A> type,LinkOption... options) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Map<String, Object> readAttributes(Path path,String attributes,LinkOption... options) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void setAttribute(Path path,String attribute,Object value,LinkOption... options) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    /* *************************************************************************************/
    /*private helper methods*/
    private org.apache.hadoop.fs.Path toHPath(Path path){
        return new org.apache.hadoop.fs.Path(path.toUri());
    }


    private class HFileInfo implements FileInfo{
        private final boolean isDir;
        private final AclStatus aclStatus;
        private final boolean isReadable;
        private final boolean isWritable;
        private org.apache.hadoop.fs.Path path;
        private ContentSummary contentSummary;

        public HFileInfo(org.apache.hadoop.fs.Path path,ContentSummary contentSummary) throws IOException{
            this.path=path;
            this.contentSummary=contentSummary;
            this.isDir = fs.isDirectory(path);
            AclStatus aclS;
            try{
                aclS=fs.getAclStatus(path);
            } catch (UnsupportedOperationException | AclException e) { // Runtime Exception for RawFS
                aclS = new AclStatus.Builder().owner("unknown").group("unknown").build();
            } catch(Exception e){
                e = exceptionFactory.processRemoteException(e); //strip any multi-retry errors out
                //noinspection ConstantConditions
                if(e instanceof UnsupportedOperationException|| e instanceof AclException){
                    /*
                     * Some Filesystems don't support aclStatus. In that case,
                     * we replace it with our own ACL status object
                     */
                    aclS=new AclStatus.Builder().owner("unknown").group("unknown").build();
                }else{
                    /*
                     * the remaining errors are of the category of FileNotFound,UnresolvedPath,
                     * etc. These are environmental, so we should throw up here.
                     */
                    throw new IOException(e);
                }
            }
            this.aclStatus = aclS;
            boolean readable;
            try{
                fs.access(path,FsAction.READ);
                readable= true;
            }catch(IOException ioe){
               readable = false;
            }
            boolean writable;
            try{
                fs.access(path,FsAction.WRITE);
                writable = true;
            }catch(IOException ioe){
                writable = false;
            }
            this.isReadable = readable;
            this.isWritable = writable;
        }

        @Override
        public String fileName(){
            return path.getName();
        }

        @Override
        public String fullPath(){
            return path.toString();
        }

        @Override
        public boolean isDirectory(){
            return isDir;
        }

        @Override
        public long fileCount(){
            return contentSummary.getFileCount();
        }

        @Override
        public long spaceConsumed(){
            return contentSummary.getSpaceConsumed();
        }

        @Override
        public long size(){
            return contentSummary.getLength();
        }

        @Override
        public boolean isReadable(){
            return isReadable;
        }

        @Override
        public String getUser(){
            return aclStatus.getOwner();
        }

        @Override
        public String getGroup(){
            return aclStatus.getGroup();
        }

        @Override
        public boolean isWritable(){
            return isWritable;
        }

        @Override
        public String toSummary() { // FileUtils.byteCountToDisplaySize
            StringBuilder sb = new StringBuilder();
            sb.append(this.isDirectory() ? "Directory = " : "File = ").append(fullPath());
            sb.append("\nFile Count = ").append(contentSummary.getFileCount());
            sb.append("\nSize = ").append(FileUtils.byteCountToDisplaySize(this.size()));
            // Not important to display here, but keep it around in case.
            // For import we only care about the actual file size, not space consumed.
            // if (this.spaceConsumed() != this.size())
            //     sb.append("\nSpace Consumed = ").append(FileUtils.byteCountToDisplaySize(this.spaceConsumed()));
            return sb.toString();
        }

        @Override
        public boolean exists(){
            try{
                return fs.exists(path);
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }
    }
}
