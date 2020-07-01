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

package com.splicemachine.si.impl;

import com.splicemachine.access.api.DistributedFileOpenOption;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.List;
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
        Pattern pattern = Pattern.compile(filePattern.replace("*", ".*"));
        try (DirectoryStream<Path> stream =
                     localDelegate.newDirectoryStream(
                             getPath(dir),
                             entry -> pattern.matcher(entry.getFileName().toString()).matches())) {
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
        public boolean isEmptyDirectory()
        {
            if(!isDirectory()) return false;
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(p)) {
                for (Path p : directoryStream) {
                    if (p.getFileName().equals("_SUCCESS") || p.getFileName().equals("_SUCCESS.crc") ) continue;
                    return false;
                }
                return true;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
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

