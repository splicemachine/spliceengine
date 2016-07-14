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
