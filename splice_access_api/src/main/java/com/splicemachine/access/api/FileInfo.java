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

package com.splicemachine.access.api;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public interface FileInfo{

    String fileName();

    String fullPath();

    boolean isDirectory();

    /** returns true if isDirectory() and (directory is empty or only contains one file _SUCCESS) */
    boolean isEmptyDirectory() throws IOException;

    /**
     * @return the number of files in the directory, or 1 if this is a file.
     * Note: this is SLOW on big directory trees when using remote filesystems like S3,
     * since requiring a full recursive listdir
     * If just need to check for empty directory, use {@link #isEmptyDirectory()}
     */
    long recursiveFileCount() throws IOException;


    /**
     * Returns the overall space consumed for the file.
     * Depends on the file system. For HDFS, this would return
     * not the current size of the file but rather
     * current size * replication factor. For a local system,
     * it would return the same value as {@link #recursiveSize()},
     * the actual current size of the file.
     * Note: this is SLOW on big directory trees when using remote filesystems like S3,
     * since requiring a full recursive listdir
     */
    long spaceConsumed() throws IOException;

    /**
     * list recursively all files
     */
    FileInfo[] listFilesRecursive();
    FileInfo[] listDir() throws IOException;

    /**
     *  Note: this is SLOW on big directory trees when using remote filesystems like S3,
     * since requiring a full recursive listdir
     */
    long recursiveSize() throws IOException;

    boolean isReadable();

    String getUser();
    String getGroup();

    boolean isWritable();

    String toSummary() throws IOException;

    boolean exists();

    String getPermissionStr() throws IOException;
    long getModificationTime();
    long size() throws IOException;
}
