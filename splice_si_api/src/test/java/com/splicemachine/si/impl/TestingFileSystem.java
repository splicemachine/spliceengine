/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

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

    public void delete(Path path) throws IOException{
        localDelegate.delete(path);
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
        final Pattern pattern = Pattern.compile(filePattern);
        try (DirectoryStream<Path> stream =
                     localDelegate.newDirectoryStream(
                             getPath(dir),
                             new DirectoryStream.Filter<Path>() {
                                 @Override
                                 public boolean accept(Path entry) throws IOException {
                                     return pattern.matcher(entry.getFileName().toString()).matches();
                                 }
                             })) {
            List<String> files = new ArrayList<>();
            for (Path i : stream) {
                files.add(i.toAbsolutePath().toString());
            }
            return files.toArray(new String[files.size()]);
        }
    }

    public Path getPath(String directory,String fileName){
        return Paths.get(directory,fileName);
    }

    public Path getPath(String fullPath){
        return Paths.get(fullPath);
    }

    @Override
    public String getFileName(String fullPath) {
        return getPath(fullPath).getFileName().toString();
    }

    @Override
    public boolean exists(String fullPath) throws IOException {
        return Files.exists(getPath(fullPath));
    }

    @Override
    public FileInfo getInfo(String filePath){
        Path p = getPath(filePath);
        return new PathInfo(p);
    }

    @Override
    public OutputStream newOutputStream(String dir,String fileName,OpenOption... options) throws IOException{
        return localDelegate.newOutputStream(Paths.get(dir, fileName),options);
    }

    @Override
    public OutputStream newOutputStream(String fullPath,OpenOption... options) throws IOException{
        Path path = Paths.get(fullPath);
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
    public InputStream newInputStream(String fullPath, OpenOption... options) throws IOException {
        return localDelegate.newInputStream(Paths.get(fullPath), options);
    }

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
    public boolean createDirectory(String fullPath,boolean errorIfExists) throws IOException {
        return createDirectory(getPath(fullPath), errorIfExists);
    }
    public void copy(Path source,Path target,CopyOption... options) throws IOException{
        localDelegate.copy(source,target,options);
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

