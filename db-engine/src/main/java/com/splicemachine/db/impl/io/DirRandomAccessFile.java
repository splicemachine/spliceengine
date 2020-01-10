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

package com.splicemachine.db.impl.io;

import com.splicemachine.db.io.StorageRandomAccessFile;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.FileNotFoundException;

/**
 * This class provides a disk based implementation of the StIRandomAccess File interface. It is used by the
 * database engine to access persistent data and transaction logs under the directory (default) subsubprotocol.
 */
class DirRandomAccessFile extends RandomAccessFile implements StorageRandomAccessFile
{

    /**
     * Construct a StorageRandomAccessFileImpl.
     *
     * @param name The file name.
     * @param mode The file open mode: "r", "rw", "rws", or "rwd". The  "rws" and "rwd" modes specify that the file is to
     *             be synchronized, consistent with the java.io.RandomAccessFile class. However the
     *             StorageRandomAccessFile.sync() method will be called even if the file was opened
     *             in "rws" or "rwd" mode.  If the "rws" or "rwd" modes are supported then the implementation
     *             of StorageRandomAccessFile.sync need not do anything.
     *
     * @exception IllegalArgumentException if the mode argument is not equal to one of "r", "rw".
     * @exception FileNotFoundException if the file exists but is a directory rather than a regular
     *              file, or cannot be opened or created for any other reason .
     */
    DirRandomAccessFile( File name, String mode) throws FileNotFoundException
    {
        super( name, mode);
    }

    /**
     * Force any changes out to the persistent store.
     *
     * @exception IOException If an IO error occurs.
     */
    public void sync( ) throws IOException
    {
        getFD().sync();
    }
}
    
