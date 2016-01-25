package com.splicemachine.access.api;

import java.io.IOException;
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
}
