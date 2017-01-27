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
