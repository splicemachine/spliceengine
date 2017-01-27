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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.io;

import com.splicemachine.db.io.WritableStorageFactory;
import com.splicemachine.db.io.StorageFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.SyncFailedException;

/**
 * This class provides a disk based implementation of the StorageFactory interface. It is used by the
 * database engine to access persistent data and transaction logs under the directory (default) subsubprotocol.
 */

public class DirStorageFactory extends BaseStorageFactory
    implements WritableStorageFactory
{
    /**
     * Construct a StorageFile from a path name.
     *
     * @param path The path name of the file
     *
     * @return A corresponding StorageFile object
     */
    public final StorageFile newStorageFile( String path)
    {
        return newPersistentFile( path);
    }
    
    /**
     * Construct a StorageFile from a directory and file name.
     *
     * @param directoryName The directory part of the path name.
     * @param fileName The name of the file within the directory.
     *
     * @return A corresponding StorageFile object
     */
    public final StorageFile newStorageFile( String directoryName, String fileName)
    {
       return newPersistentFile( directoryName, fileName);
    }
    
    /**
     * Construct a StorageFile from a directory and file name.
     *
     * @param directoryName The directory part of the path name.
     * @param fileName The name of the file within the directory.
     *
     * @return A corresponding StorageFile object
     */
    public final StorageFile newStorageFile( StorageFile directoryName, String fileName)
    {
        return newPersistentFile( directoryName, fileName);
    }
    /**
     * Construct a persistent StorageFile from a path name.
     *
     * @param path The path name of the file. Guaranteed not to be in the temporary file directory. If null
     *             then the database directory should be returned.
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( String path)
    {
        if( path == null)
            return new DirFile( dataDirectory);
        return new DirFile(dataDirectory, path);
    }

    /**
     * Construct a persistent StorageFile from a directory and path name.
     *
     * @param directoryName The path name of the directory. Guaranteed not to be in the temporary file directory.
     *                  Guaranteed not to be null
     * @param fileName The name of the file within the directory. Guaranteed not to be null.
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( String directoryName, String fileName)
    {
        return new DirFile( separatedDataDirectory + directoryName, fileName);
    }

    /**
     * Construct a persistent StorageFile from a directory and path name.
     *
     * @param directoryName The path name of the directory. Guaranteed not to be to be null. Guaranteed to be
     *                  created by a call to one of the newPersistentFile methods.
     * @param fileName The name of the file within the directory. Guaranteed not to be null.
     *
     * @return A corresponding StorageFile object
     */
    StorageFile newPersistentFile( StorageFile directoryName, String fileName)
    {
        return new DirFile( (DirFile) directoryName, fileName);
    }

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
    public void sync( OutputStream stream, boolean metaData) throws IOException, SyncFailedException
    {
        ((FileOutputStream) stream).getFD().sync();
    }

    /**
     * This method tests whether the "rws" and "rwd" modes are implemented. If
     * the "rws" and "rwd" modes are supported then the database engine will
     * conclude that the write methods of "rws"/"rwd" mode
     * StorageRandomAccessFiles are slow but the sync method is fast and
     * optimize accordingly.
     *
     * @return <b>true</b> if an StIRandomAccess file opened with "rws" or "rwd" modes immediately writes data to the
     *         underlying storage, <b>false</b> if not.
     */
    public boolean supportsWriteSync()
    {
        return false;
    }

    public boolean isReadOnlyDatabase()
    {
        return false;
    }

    /**
     * Determine whether the storage supports random access. If random access is not supported then
     * it will only be accessed using InputStreams and OutputStreams (if the database is writable).
     *
     * @return <b>true</b> if the storage supports random access, <b>false</b> if it is writable.
     */
    public boolean supportsRandomAccess()
    {
        return true;
    }

    void doInit() throws IOException
    {
        if( dataDirectory != null)
        {
            File dataDirectoryFile = new File( dataDirectory);
            File databaseRoot = null;
            if( dataDirectoryFile.isAbsolute())
                databaseRoot = dataDirectoryFile;
            else if( home != null && dataDirectory.startsWith( home))
                databaseRoot = dataDirectoryFile;
            else
            {
                databaseRoot = new File( home, dataDirectory);
                if (home != null)
                    dataDirectory = home + getSeparator() +  dataDirectory;
            }
            canonicalName = databaseRoot.getCanonicalPath();
            createTempDir();
            separatedDataDirectory = dataDirectory + getSeparator();
        }
        else if( home != null)
        {
            File root = new File( home);
            dataDirectory = root.getCanonicalPath();
            separatedDataDirectory = dataDirectory + getSeparator();
        }
    } // end of doInit
}
