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

package com.splicemachine.access.api;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public abstract class DistributedFileSystem extends FileSystemProvider{

    public abstract void delete(Path path,boolean recursive) throws IOException;

    public abstract void delete(String directory,boolean recursive) throws IOException;

    public abstract void delete(String directory,String fileName,boolean recursive) throws IOException;

    /**
     * Fetches the names of files in the specified directory that match the specified pattern,
     * which can include wildcards.
     *
     * @param dir directory to search for files
     * @param filePattern pattern to use for matching files
     * @return array of file names (just the names, not the full paths)
     * @throws IOException
     */
    public abstract String[] getExistingFiles(String dir, String filePattern) throws IOException;

    public abstract Path getPath(String directory,String fileName);

    public abstract Path getPath(String fullPath);

    public abstract FileInfo getInfo(String filePath) throws IOException;

    public abstract OutputStream newOutputStream(String dir, String fileName, OpenOption... options) throws IOException;

    public abstract OutputStream newOutputStream(String fullPath, OpenOption... options) throws IOException;

    /**
     * Creates the specified directory.
     *
     * @param path the directory
     * @param errorIfExists if {@code true}, then throw an error if the directory already exists;
     *                      if {@code false}, no error is thrown
     * @return whether the creation was successful
     * @throws IOException
     */
    public abstract boolean createDirectory(Path path,boolean errorIfExists) throws IOException;

    public abstract boolean createDirectory(String fullPath,boolean errorIfExists) throws IOException;

    /**
     * Create a new, empty file at the specified location.
     *
     * @param path the location to create the empty file at.
     * @throws java.nio.file.FileAlreadyExistsException if the file already exists
     * @throws IOException if something generically goes wrong.
     */
    public abstract void touchFile(Path path) throws IOException;

    public abstract void touchFile(String dir, String fileName) throws IOException;

    /**
     * Append sources to target, deleting sources after the copy.
     * @param target path to which to write. Expecting exists and is writable.
     * @param sources paths from which to read.  Expecting exists and are readable.
     *                These will be deleted.
     */
    public void concat(Path target, Path... sources)  throws IOException {
        throw new UnsupportedOperationException("IMPLEMENT concat");
    }
}
