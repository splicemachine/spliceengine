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

    long spaceConsumed();

    boolean isReadable();

    String getUser();

    String getGroup();

    boolean isWritable();

    String toSummary();
}
