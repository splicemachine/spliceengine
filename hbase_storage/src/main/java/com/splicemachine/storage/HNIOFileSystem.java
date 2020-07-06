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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

import static com.google.common.collect.Iterables.toArray;

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

    public FileInfo getInfo(String filePath) throws IOException {

        return new HFileInfo( new org.apache.hadoop.fs.Path(filePath) );
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

    /* *************************************************************************************/
    /*private helper methods*/
    private org.apache.hadoop.fs.Path toHPath(Path path){
        return new org.apache.hadoop.fs.Path(path.toUri());
    }

    private class HFileInfo implements FileInfo{
        private org.apache.hadoop.fs.Path path;
        FileStatus fileStatus;
        private ContentSummary contentSummary = null; // calculate on demand

        public HFileInfo(org.apache.hadoop.fs.Path path) throws IOException{
            this.path=path;
            try {
                URI uri = URI.create(path.toString());
                String scheme = uri.getScheme();
                if (scheme != null && scheme.equalsIgnoreCase("s3a") ) {
                    this.fileStatus = s3getFileStatus(path);
                }
                else {
                    this.fileStatus = fs.getFileStatus(path);
                }
            } catch( FileNotFoundException e )
            {
                this.fileStatus = null;
            }
        }

        @Override
        public String getDeepest() {
            try {
                listRoot();
                for (FileStatus a : rootFileStatusArr) {
                    if (!a.isDirectory() && !a.getPath().getName().toString().startsWith(".") && !a.getPath().getName().toString().equals("_SUCCESS"))
                        return a.getPath().toString();
                }
                return null;
            }
            catch( Exception e )
            {
                return null;
            }
        }

        private FileStatus s3getFileStatus(org.apache.hadoop.fs.Path path) throws IOException {
            // PrestoS3AFileSystem has problem reading an empty folder. It cannot determine whether the folder does not
            // exist, or the folder is empty. Skip checking for S3A. If the directory does not exist, it will be
            // created.
            listRoot();

            if( rootFileStatusArr.length > 1 ) {
                // a directory
                long maxLastModifiedTime = Arrays.stream(rootFileStatusArr)
                                                .map( f -> f.getModificationTime() ).max(Long::compare).orElse((long) 0);
                return new FileStatus(0,true,1, 0, maxLastModifiedTime, path);
            }
            else if( rootFileStatusArr.length > 0 ){
                return rootFileStatusArr[0];
            }
            else
            {
                return null;
            }
        }

        // these two methods are to avoid having to re-calculate the list of files in the directory
        // for isEmptyDirectory
        private FileStatus rootFileStatusArr[];
        private FileStatus[] listRoot() throws IOException {
            if (rootFileStatusArr != null) return rootFileStatusArr;
            List<LocatedFileStatus> list = new ArrayList<>();
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, true); // recursive!
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
            rootFileStatusArr = toArray(list, LocatedFileStatus.class);
            return rootFileStatusArr;
        }

        private ContentSummary getContentSummary() {
            if( contentSummary != null ) return contentSummary;
            try
            {
                if (fileStatus.isFile()) {
                    // f is a file
                    long length = fileStatus.getLen();
                    contentSummary = new ContentSummary.Builder().length(length).
                            fileCount(1).directoryCount(0).spaceConsumed(length).build();
                }
                else {
                    FileStatus[] all = listRoot();
                    Set<String> directories = new HashSet<>();
                    long length = 0, fileCount = 0;
                    for (FileStatus a : all) {
                        directories.add(a.getPath().getParent().toString());
                        length += a.getLen();
                        fileCount++;
                    }
                    contentSummary = new ContentSummary(length, fileCount, directories.size());
                }
            }
            catch (IOException ioe) {
                LOG.error("Unexpected error getting content summary. We ignore it for now, but you should probably check it out:", ioe);
                contentSummary = new ContentSummary(0L, 0L, 0L);
            }
            return contentSummary;
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
            return fileStatus != null && fileStatus.isDirectory();
        }

        @Override
        public long fileCount(){
            if( !exists() ) return 0;
            return getContentSummary().getFileCount();
        }

        @Override
        public boolean isEmptyDirectory() {
            if( !exists() ) return false;
            if( !isDirectory() ) return false;
            try {
                for (FileStatus s : listRoot() ) {
                    if (s.getPath().getName().equals("_SUCCESS") || s.getPath().getName().equals("_SUCCESS.crc") ) continue;
                    return false;
                }
                return true;
            } catch( Exception e ) {
                // this shouldn't happen, as we already check if it exists.
                LOG.error("Unexpected error listing directory", e);
                return false;
            }
        }

        @Override
        public long spaceConsumed(){
            if( !exists() ) return 0;
            return getContentSummary().getSpaceConsumed();
        }

        @Override
        public long size(){
            if( !exists() ) return 0;
            return getContentSummary().getLength();
        }

        @Override
        public boolean isReadable(){
            if( !exists() ) return false;
            return fileStatus.getPermission().getUserAction().implies(FsAction.READ);
        }

        @Override
        public String getUser(){
            if( !exists() ) return "";
            return fileStatus.getOwner();
        }

        @Override
        public String getGroup(){
            if( !exists() ) return "";
            return fileStatus.getGroup();
        }

        @Override
        public boolean isWritable(){
            if( !exists() ) return false;
            return fileStatus.getPermission().getUserAction().implies(FsAction.WRITE);
        }

        @Override
        public String toSummary() {
            if( !exists() ) return "file not found " + fullPath();
            StringBuilder sb = new StringBuilder();
            sb.append(this.isDirectory() ? "Directory = " : "File = ").append(fullPath());
            if( !isDirectory() ) {
                // this is slow for directories (needs recursive scan), so just do for files
                sb.append("\nSize = ").append(FileUtils.byteCountToDisplaySize(this.size()));
                // Not important to display here, but keep it around in case.
                // For import we only care about the actual file size, not space consumed.
                // if (this.spaceConsumed() != this.size())
                //     sb.append("\nSpace Consumed = ").append(FileUtils.byteCountToDisplaySize(this.spaceConsumed()));
            }

            return sb.toString();
        }

        @Override
        public boolean exists(){
            return fileStatus != null;
        }
    }
}
