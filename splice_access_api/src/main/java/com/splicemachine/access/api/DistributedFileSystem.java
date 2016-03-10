package com.splicemachine.access.api;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public abstract class DistributedFileSystem extends FileSystemProvider{

    public abstract void delete(Path path, boolean recursive) throws IOException;

    public abstract Path getPath(String directory,String fileName);

    public abstract Path getPath(String fullPath);

    public abstract FileInfo getInfo(String filePath) throws IOException;

    /**
     *
     * @param path
     * @param errorIfExists if {@code true}, then throw an error if the directory already exists;
     *                      if {@code false}, no error is thrown
     * @return
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
}
