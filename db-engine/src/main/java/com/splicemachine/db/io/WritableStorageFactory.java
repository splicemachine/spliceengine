/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.io;

import java.io.OutputStream;
import java.io.IOException;
import java.io.SyncFailedException;

/**
 * This interface extends StorageFactory to provide read/write access to storage.
 *<p>
 * The database engine will call this interface's methods from its own privilege blocks.
 *<p>
 * Each WritableStorageFactory instance may be concurrently used by multiple threads.
 *
 */
public interface WritableStorageFactory extends StorageFactory
{


    /**
     * Force the data of an output stream out to the underlying storage. That is, ensure that
     * it has been made persistent. If the database is to be transient, that is, if the database
     * does not survive a restart, then the sync method implementation need not do anything.
     *
     * @param stream The stream to be synchronized.
     * @param metaData If true then this method must force both changes to the file's
     *          contents and metadata to be written to storage; if false, it need only force file content changes
     *          to be written. The implementation is allowed to ignore this parameter and always force out
     *          metadata changes.
     *
     * @exception IOException if an I/O error occurs.
     * @exception SyncFailedException Thrown when the buffers cannot be flushed,
     *            or because the system cannot guarantee that all the buffers have been
     *            synchronized with physical media.
     */
    void sync(OutputStream stream, boolean metaData) throws IOException, SyncFailedException;

    /**
     * This method tests whether the StorageRandomAccessFile "rws" and "rwd" modes
     * are implemented. If the "rws" and "rwd" modes are supported then the database
     * engine will conclude that the write methods of "rws"/"rwd" mode
     * StorageRandomAccessFiles are slow but the sync method is fast and optimize
     * accordingly.
     *
     * @return <b>true</b> if an StIRandomAccess file opened with "rws" or "rwd" modes immediately writes data to the
     *         underlying storage, <b>false</b> if not.
     */
    boolean supportsWriteSync();
}
