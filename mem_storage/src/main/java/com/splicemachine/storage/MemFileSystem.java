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
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Scott Fines
 *         Date: 1/18/16
 */
@SuppressFBWarnings(value={ "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"}, justification = ".")
public class MemFileSystem extends DistributedFileSystem{
    private final FileSystemProvider localDelegate;
    private static Logger LOG=Logger.getLogger(MemFileSystem.class);

    public MemFileSystem(FileSystemProvider localDelegate){
        this.localDelegate=localDelegate;
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

    @Override
    public String getFileName(String fullPath) {
        return getPath(fullPath).getFileName().toString();
    }

    @Override
    public boolean exists(String fullPath) throws IOException {
        return Files.exists(getPath(fullPath));
    }

    public Path getPath(String directory,String fileName){
        return Paths.get(directory,fileName);
    }

    public Path getPath(String fullPath){
        return Paths.get(fullPath);
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
        return localDelegate.newOutputStream(Paths.get(fullPath),options);
    }

    @Override
    public InputStream newInputStream(String fullPath, OpenOption... options) throws IOException {
        return localDelegate.newInputStream(Paths.get(fullPath),options);
    }

    public boolean createDirectory(Path dir,boolean errorIfExists) throws IOException{
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "createDirectory(): path = %s", dir);

        if (Files.exists(dir))
            return true;
        try {
            localDelegate.createDirectory(dir);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean createDirectory(String fullPath,boolean errorIfExists) throws IOException {
        Path path = getPath(fullPath);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "createDirectory(): path = %s", path);

        return createDirectory(path, errorIfExists);
    }

    @Override
    public void touchFile(String dir, String fileName) throws IOException{
        Files.createFile(Paths.get(dir, fileName));
    }

    public void copy(Path source,Path target,CopyOption... options) throws IOException{
        localDelegate.copy(source,target,options);
    }

    private static class PathInfo implements FileInfo{
        private final Path p;

        public PathInfo(Path p){
            assert p!=null: "Cannot create info with a null path!";
            this.p=p;
        }

        @Override
        public String fileName(){
            Path fileName=p.getFileName();
            assert fileName!=null: "Programmer error: no file name present!";
            return fileName.toString();
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
                    if ( p == null || p.getFileName() == null ) continue;
                    String name = p.getFileName().toString();
                    if( name.equals("_SUCCESS") || name.equals("_SUCCESS.crc") ) continue;
                    return false;
                }
                return true;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public long fileCount(){
            if(!isDirectory()) return 1l;
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
            try{
                PosixFileAttributes attrs = Files.getFileAttributeView(p,PosixFileAttributeView.class).readAttributes();
                return attrs.owner().getName();
            }catch(IOException e){
                throw new RuntimeException(e);
            }
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
