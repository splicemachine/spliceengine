package com.splicemachine.access.api;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public interface FileInfo{

    String fileName();

    String fullPath();

    boolean isDirectory();

    /**
     * @return the number of files in the directory, or 1 if this is a file.
     */
    long fileCount();

    /**
     * Returns the overall space consumed for the file.
     * Depends on the file system. For HDFS, this would return
     * not the current size of the file but rather
     * current size * replication factor. For a local system,
     * it would return the same value as {@link #size()},
     * the actual current size of the file.
     */
    long spaceConsumed();

    long size();

    boolean isReadable();

    String getUser();

    String getGroup();

    boolean isWritable();

    String toSummary();

    boolean exists();
}
