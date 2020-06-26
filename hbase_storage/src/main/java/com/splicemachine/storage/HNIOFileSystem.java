/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.log4j.Logger;
import sun.reflect.annotation.ExceptionProxy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.*;

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
                files[index++] = status.getPath().toString();
        }
        return files;
    }

    @Override
    public String getFileName(String fullPath) {
        org.apache.hadoop.fs.Path p=new org.apache.hadoop.fs.Path(fullPath);
        return p.getName();
    }

    @Override
    public boolean exists(String fullPath) throws IOException {
        org.apache.hadoop.fs.Path p=new org.apache.hadoop.fs.Path(fullPath);
        return fs.exists(p);
    }
    @Override
    public FileInfo getInfo(String filePath) throws IOException {
        return getInfo_2(filePath);
    }

    public FileInfo getInfo_ori(String filePath) throws IOException{
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
    public FileInfo getInfo_3(String filePath) throws IOException {

        return new HFileInfo2( new org.apache.hadoop.fs.Path(filePath) );
    }

    public FileInfo getInfo_2(String filePath) throws IOException {
        Configuration conf = com.splicemachine.access.HConfiguration.unwrapDelegate();
        FileStatus fileInfo = null;
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(URI.create(filePath), conf);
        boolean isEmpty = true;
        try {
            org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(filePath);
            fileInfo = fs.getFileStatus(p);

            if(fileInfo.isDirectory())
            {
                int count = 1;
                int size = 0;
                // todo: calculate this on request
                boolean full_info = false;
                if( full_info ) {
                    // don't do. this takes a LONG time on s3.
                    RemoteIterator<LocatedFileStatus> it = fs.listFiles(p, true);
                    while (it.hasNext()) {
                        LocatedFileStatus fileStatus = it.next();
                        size += fileStatus.getLen();
                        count++;
                    }
                }
//                FileStatus fileInfos[] = fs.listStatus(new org.apache.hadoop.fs.Path(filePath));
//                return new MyFileInfo(true, filePath, true, fileInfos.length);
                return new MyFileInfo(true, filePath, true, count, size);
            }
            else
            {
                return new MyFileInfo(true, filePath, false, 0, 0);
            }
        } catch (FileNotFoundException fnfe) {
            return new MyFileInfo(false, filePath, false, 0, 0);
        }
    }

    class MyFileInfo implements FileInfo {
        String filename;
        boolean directory;
        int numFiles, size;
        boolean bExists;
        public MyFileInfo(boolean bExists, String filename, boolean directory, int numFiles, int size)
        {
            this.filename = filename;
            this.directory = directory;
            this.numFiles = numFiles;
            this.bExists = bExists;
            this.size = size;
        }
        @Override
        public String fileName() {
            return filename;
        }

        @Override
        public String fullPath() {
            return filename;
        }

        @Override
        public boolean isDirectory() {
            return directory;
        }

        @Override
        public long fileCount_SLOW() {
            return numFiles;
        }

        // different only on replicated filesystems like HDFS
        @Override
        public long spaceConsumed_SLOW() {
            return size_SLOW();
        }

        @Override
        public long size_SLOW() {
            return size;
        }

        @Override
        public boolean isEmptyDirectory() {
            return isDirectory() && fileCount_SLOW() == 0;
        }

        @Override
        public boolean isReadable() {
            return false;
        }

        @Override
        public String getUser() {
            return null;
        }

        @Override
        public String getGroup() {
            return null;
        }

        @Override
        public boolean isWritable() {
            return false;
        }

        @Override
        public String toSummary() {
            return null;
        }

        @Override
        public boolean exists() {
            return bExists;
        }
    }

    public FileSystem getFileSystem(URI uri){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    public Path getPath(URI uri){
        return Paths.get(uri);
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
    public InputStream newInputStream(String fullPath, OpenOption... options) throws IOException {
        org.apache.hadoop.fs.Path path=new org.apache.hadoop.fs.Path(fullPath);
        return fs.open(path);
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
        public boolean isEmptyDirectory() {
            return isDirectory() && fileCount_SLOW() == 0;
        }

        @Override
        public long fileCount_SLOW(){
            return contentSummary.getFileCount();
        }

        @Override
        public long spaceConsumed_SLOW(){
            return contentSummary.getSpaceConsumed();
        }

        @Override
        public long size_SLOW(){
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
            sb.append("\nSize = ").append(FileUtils.byteCountToDisplaySize(this.size_SLOW()));
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


    private class HFileInfo2 implements FileInfo{
        private org.apache.hadoop.fs.Path path;
        FileStatus fileStatus;
        private ContentSummary contentSummary = null; // calculate on demand

        public HFileInfo2(org.apache.hadoop.fs.Path path) throws IOException{
            this.path=path;
            this.fileStatus = fs.getFileStatus(path); // will also throw FileNotFoundException, so no need for bExists
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
            return fileStatus.isDirectory();
        }

        // note this is expensive for deeply nested directories. avoid calling fileCount, spaceConsumed and size
        void calcContentSummary() {
            if( contentSummary != null ) return;
            try {
                contentSummary = fs.getContentSummary(path);
            } catch (IOException ioe) {
                LOG.error("Unexpected error getting content summary. We ignore it for now, but you should probably check it out:", ioe);
                contentSummary = new ContentSummary(0L, 0L, 0L);
            }
        }

        @Override
        public long fileCount_SLOW(){
            calcContentSummary();
            return contentSummary.getFileCount();
        }

        @Override
        public boolean isEmptyDirectory() {
            if( !isDirectory() ) return false;
            if( contentSummary != null ) return contentSummary.getFileCount() == 0;
//            fs.listFiles( false)
            try {
                for (FileStatus s : fs.listStatus(path)) {
                    if (s.getPath().getName().equals("_SUCCESS")) continue;
                    return false;
                }
                return true;
            } catch( Exception e ) {
                // this shouldn't happen, as we already check if it exists.
                return false;
            }
        }

        @Override
        public long spaceConsumed_SLOW(){
            calcContentSummary();
            return contentSummary.getSpaceConsumed();
        }

        @Override
        public long size_SLOW(){
            calcContentSummary();
            return contentSummary.getLength();
        }

        @Override
        public boolean isReadable(){
            return fileStatus.getPermission().getUserAction().implies(FsAction.READ);
        }

        @Override
        public String getUser(){
            return fileStatus.getOwner();
        }

        @Override
        public String getGroup(){
            return fileStatus.getGroup();
        }

        @Override
        public boolean isWritable(){
            return fileStatus.getPermission().getUserAction().implies(FsAction.WRITE);
        }

        @Override
        public String toSummary() { // FileUtils.byteCountToDisplaySize
            StringBuilder sb = new StringBuilder();
            sb.append(this.isDirectory() ? "Directory = " : "File = ").append(fullPath());
//            sb.append("\nFile Count = ").append(contentSummary.getFileCount());
//            sb.append("\nSize = ").append(FileUtils.byteCountToDisplaySize(this.size_SLOW()));
            // Not important to display here, but keep it around in case.
            // For import we only care about the actual file size, not space consumed.
            // if (this.spaceConsumed() != this.size())
            //     sb.append("\nSpace Consumed = ").append(FileUtils.byteCountToDisplaySize(this.spaceConsumed()));
            return sb.toString();
        }

        @Override
        public boolean exists(){
            // constructor will throw before exists get called, so this is always true
           return true;
        }
    }
}
