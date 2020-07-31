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
import java.util.stream.Stream;

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
        private FileStatus fileStatus;
        private ContentSummary contentSummary;               // calculated on demand
        private List<LocatedFileStatus> rootFileStatusList;  // calculated on demand

        public HFileInfo(org.apache.hadoop.fs.Path path) throws IOException{
            this.path=path;
            try {
                this.fileStatus = fs.getFileStatus(path);
            } catch( FileNotFoundException e )
            {
                this.fileStatus = null;
            }
        }

        // these two methods are to avoid having to re-calculate the list of files in the directory
        // for isEmptyDirectory, size() and fileCount()
        private List<LocatedFileStatus> listRoot() throws IOException {
            if (rootFileStatusList != null) return rootFileStatusList;
            rootFileStatusList = new ArrayList<>();
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, true); // recursive!
            while (iterator.hasNext()) {
                rootFileStatusList.add(iterator.next());
            }
            return rootFileStatusList;
        }

        private ContentSummary getContentSummary() {
            if( contentSummary != null ) return contentSummary;
            try
            {
                long directories = 0, fileCount = 0, length = 0;
                if (fileStatus.isFile()) {
                    // f is a file
                    length      = fileStatus.getLen();
                    fileCount   = 1;
                    directories = 0;
                }
                else {
                    length      = listRoot().stream().map( s -> s.getLen() ).reduce(Long::sum).orElse((long) 0);
                    fileCount   = listRoot().stream().count();
                    directories = listRoot().stream().map( s -> s.getPath().getParent() ).distinct().count();
                }
                contentSummary = new ContentSummary(length, fileCount, directories);
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

        // Note: we need to be sure that the underlying filesystem can support recursive listdir efficiently.
        // this is done for S3 and HDFS has this anyhow. If this is not the case, it would be better to not use
        // (cached, but recursive) listRoot() here, but to just list the root (without recursive).
        private List<FileStatus> rootFileStatusFlat;

        @Override
        public boolean isEmptyDirectory() {
            if( !exists() ) return false;
            if( !isDirectory() ) return false;
            try {
                List<? extends FileStatus> files;
                if(rootFileStatusList != null )
                    files = rootFileStatusList;
                else
                {
                    if( rootFileStatusFlat == null )
                        rootFileStatusFlat = Arrays.asList(fs.listStatus(path));
                    files = rootFileStatusFlat;
                }
                for (FileStatus s : files ) {
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
