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

import com.splicemachine.db.io.StorageFile;

/**
 * This class implements the WritableStorageFactory interface using features found in Java 1.4 but
 * not in earlier versions of Java.
 */
public class DirStorageFactory4 extends DirStorageFactory
{

    /**
     * Most of the initialization is done in the init method.
     */
    public DirStorageFactory4()
    {
        super();
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
            return new DirFile4(dataDirectory);
        return new DirFile4(dataDirectory, path);
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
        return new DirFile4( separatedDataDirectory + directoryName, fileName);
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
        return new DirFile4((DirFile) directoryName, fileName);
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
        return true;
    }
	

}
