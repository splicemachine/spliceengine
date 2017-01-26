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

package com.splicemachine.si.impl;

import com.splicemachine.access.api.DistributedFileOpenOption;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * A convenient Local file system for use in testing code without needing to resort to an architecture
 * specific file system during testing.
 * @author Scott Fines
 *         Date: 1/18/16
 */
public class TestingFileSystem extends DistributedFileSystem{
    private final FileSystemProvider localDelegate;

    public TestingFileSystem(FileSystemProvider localDelegate){
        this.localDelegate=localDelegate;
    }

    @Override
    public void delete(Path path,boolean recursive) throws IOException{
        //TODO -sf- deal with recursive deletes
        localDelegate.delete(path);
    }

    @Override
    public void delete(Path path) throws IOException{
        localDelegate.delete(path);
    }

    @Override
    public boolean deleteIfExists(Path path) throws IOException{
        return localDelegate.deleteIfExists(path);
    }

    @Override
    public void delete(String dir,boolean recursive) throws IOException{
        localDelegate.delete(getPath(dir));
    }

    @Override
    public void delete(String dir,String fileName,boolean recursive) throws IOException{
        localDelegate.delete(getPath(dir,fileName));
    }

    public String[] getExistingFiles(String dir, String filePattern) throws IOException {
        return new String[]{};
    };

    @Override
    public Path getPath(String directory,String fileName){
        return Paths.get(directory,fileName);
    }

    @Override
    public Path getPath(String fullPath){
        return Paths.get(fullPath);
    }

    @Override
    public FileInfo getInfo(String filePath){
        Path p = getPath(filePath);
        return new PathInfo(p);
    }


    /*delegate methods*/
    @Override
    public String getScheme(){
        return localDelegate.getScheme();
    }

    @Override
    public FileSystem newFileSystem(URI uri,Map<String, ?> env) throws IOException{
        return localDelegate.newFileSystem(uri,env);
    }

    @Override
    public FileSystem getFileSystem(URI uri){
        return localDelegate.getFileSystem(uri);
    }

    @Override
    public Path getPath(URI uri){
        return localDelegate.getPath(uri);
    }

    @Override
    public FileSystem newFileSystem(Path path,Map<String, ?> env) throws IOException{
        return localDelegate.newFileSystem(path,env);
    }

    @Override
    public InputStream newInputStream(Path path,OpenOption... options) throws IOException{
        return localDelegate.newInputStream(path,options);
    }

    @Override
    public OutputStream newOutputStream(Path path,OpenOption... options) throws IOException{
        if(options.length==1){
            if(options[0] instanceof DistributedFileOpenOption){
                return localDelegate.newOutputStream(path,((DistributedFileOpenOption)options[0]).standardOption());
            }else
                return localDelegate.newOutputStream(path,options);
        }
        OpenOption [] directOptions = new OpenOption[options.length];
        for(int i=0;i<directOptions.length;i++){
            if(options[i] instanceof DistributedFileOpenOption){
                directOptions[i]=((DistributedFileOpenOption)options[i]).standardOption();
            }else
                directOptions[i]=options[i];
        }
        return localDelegate.newOutputStream(path,directOptions);
    }

    @Override
    public OutputStream newOutputStream(String dir,String fileName,OpenOption... options) throws IOException{
        return localDelegate.newOutputStream(Paths.get(dir, fileName),options);
    }

    @Override
    public OutputStream newOutputStream(String fullPath,OpenOption... options) throws IOException{
        return localDelegate.newOutputStream(Paths.get(fullPath),options);
    }

    @Override
    public FileChannel newFileChannel(Path path,Set<? extends OpenOption> options,FileAttribute<?>... attrs) throws IOException{
        return localDelegate.newFileChannel(path,options,attrs);
    }

    @Override
    public AsynchronousFileChannel newAsynchronousFileChannel(Path path,Set<? extends OpenOption> options,ExecutorService executor,FileAttribute<?>... attrs) throws IOException{
        return localDelegate.newAsynchronousFileChannel(path,options,executor,attrs);
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path,Set<? extends OpenOption> options,FileAttribute<?>... attrs) throws IOException{
        return localDelegate.newByteChannel(path,options,attrs);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir,DirectoryStream.Filter<? super Path> filter) throws IOException{
        return localDelegate.newDirectoryStream(dir,filter);
    }


    @Override
    public boolean createDirectory(Path dir,boolean errorIfExists) throws IOException{
        try{
            localDelegate.createDirectory(dir);
            return true;
        }catch(FileAlreadyExistsException fafe){
            if(!errorIfExists)
                return Files.isDirectory(dir);
            else{
                throw fafe;
            }
        }
    }

    @Override
    public void createDirectory(Path dir,FileAttribute<?>... attrs) throws IOException{
        try{
            localDelegate.createDirectory(dir,attrs);
        }catch(FileAlreadyExistsException fafe){
            //determine if the path is already a directory, or if it is a file. If it's a file, then
            //throw a NotADirectoryException. Otherwise, we are good
            if(!Files.isDirectory(dir)){
                throw fafe;
            }
        }
    }

    @Override
    public boolean createDirectory(String fullPath,boolean errorIfExists) throws IOException {
        return createDirectory(getPath(fullPath), errorIfExists);
    }

    @Override
    public void createSymbolicLink(Path link,Path target,FileAttribute<?>... attrs) throws IOException{
        localDelegate.createSymbolicLink(link,target,attrs);
    }

    @Override
    public void createLink(Path link,Path existing) throws IOException{
        localDelegate.createLink(link,existing);
    }

    @Override
    public Path readSymbolicLink(Path link) throws IOException{
        return localDelegate.readSymbolicLink(link);
    }

    @Override
    public void copy(Path source,Path target,CopyOption... options) throws IOException{
        localDelegate.copy(source,target,options);
    }

    @Override
    public void move(Path source,Path target,CopyOption... options) throws IOException{
        localDelegate.move(source,target,options);
    }

    @Override
    public boolean isSameFile(Path path,Path path2) throws IOException{
        return localDelegate.isSameFile(path,path2);
    }

    @Override
    public boolean isHidden(Path path) throws IOException{
        return localDelegate.isHidden(path);
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException{
        return localDelegate.getFileStore(path);
    }

    @Override
    public void checkAccess(Path path,AccessMode... modes) throws IOException{
        localDelegate.checkAccess(path,modes);
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path,Class<V> type,LinkOption... options){
        return localDelegate.getFileAttributeView(path,type,options);
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path,Class<A> type,LinkOption... options) throws IOException{
        return localDelegate.readAttributes(path,type,options);
    }

    @Override
    public Map<String, Object> readAttributes(Path path,String attributes,LinkOption... options) throws IOException{
        return localDelegate.readAttributes(path,attributes,options);
    }

    @Override
    public void setAttribute(Path path,String attribute,Object value,LinkOption... options) throws IOException{
        localDelegate.setAttribute(path,attribute,value,options);
    }

    @Override
    public void touchFile(Path path) throws IOException{
        Files.createFile(path);
    }

    @Override
    public void touchFile(String dir, String fileName) throws IOException{
        Files.createFile(Paths.get(dir, fileName));
    }

    private static class PathInfo implements FileInfo{
        private final Path p;

        public PathInfo(Path p){
            this.p=p;
        }

        @Override
        public String fileName(){
            return p.getFileName().toString();
        }

        @Override
        public String fullPath(){
            return p.toString();
        }

        @Override
        public boolean isDirectory(){
            return Files.isDirectory(p);
        }

        @Override
        public long fileCount(){
            if(!isDirectory()) return 0l;
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(p)) {
                long count = 0;
                for (Path ignored : directoryStream) {
                    count++;
                }
                return count;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public long spaceConsumed(){
            try{
                return Files.size(p);
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        public long size(){
            try{
                return Files.size(p);
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isReadable(){
            return Files.isReadable(p);
        }

        @Override
        public String getUser(){
            try{
                return Files.getOwner(p).getName();
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getGroup(){
            throw new UnsupportedOperationException("IMPLEMENT");
        }

        @Override
        public boolean isWritable(){
            return Files.isWritable(p);
        }

        @Override
        public String toSummary() {
            StringBuilder sb = new StringBuilder();
            sb.append(this.isDirectory() ? "Directory = " : "File = ").append(fullPath());
            sb.append("\nFile Count = ").append(this.fileCount());
            sb.append("\nSize = ").append(size());
            return sb.toString();
        }

        @Override
        public boolean exists(){
            return Files.exists(p);
        }

    }
}

