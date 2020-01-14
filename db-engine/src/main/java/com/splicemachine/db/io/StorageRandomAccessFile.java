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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This interface abstracts an object that implements reading and writing on a random access
 * file. It extends DataInput and DataOutput, so it implicitly contains all the methods of those
 * interfaces. Any method in this interface that also appears in the java.io.RandomAccessFile class
 * should behave as the java.io.RandomAccessFile method does.
 *<p>
 * Each StorageRandomAccessFile has an associated file pointer, a byte offset in the file. All reading and writing takes
 * place at the file pointer offset and advances it.
 *<p>
 * An implementation of StorageRandomAccessFile need not be thread safe. The database engine
 * single-threads access to each StorageRandomAccessFile instance. Two threads will not access the
 * same StorageRandomAccessFile instance at the same time.
 *<p>
 * @see <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/io/RandomAccessFile.html">java.io.RandomAccessFile</a>
 */
public interface StorageRandomAccessFile extends DataInput, DataOutput
{

    /**
     * Closes this file.
     *
     * @exception IOException - if an I/O error occurs.
     */
    void close() throws IOException;

    /**
     * Get the current offset in this file.
     *
     * @return the current file pointer. 
     *
     * @exception IOException - if an I/O error occurs.
     */
    long getFilePointer() throws IOException;

    /**
     * Gets the length of this file.
     *
     * @return the number of bytes this file. 
     *
     * @exception IOException - if an I/O error occurs.
     */
    long length() throws IOException;

    /**
     * Set the file pointer. It may be moved beyond the end of the file, but this does not change
     * the length of the file. The length of the file is not changed until data is actually written..
     *
     * @param newFilePointer the new file pointer, measured in bytes from the beginning of the file.
     *
     * @exception IOException - if newFilePointer is less than 0 or an I/O error occurs.
     */
    void seek(long newFilePointer) throws IOException;

    /**
     * Sets the length of this file, either extending or truncating it.
     *<p>
     * If the file is extended then the contents of the extension are not defined.
     *<p>
     * If the file is truncated and the file pointer is greater than the new length then the file pointer
     * is set to the new length.
     *
     * @param newLength The new file length.
     *
     * @exception IOException If an I/O error occurs.
     */
    void setLength(long newLength) throws IOException;
    
    /**
     * Force any changes out to the persistent store. If the database is to be transient, that is, if the database
     * does not survive a restart, then the sync method implementation need not do anything.
     *
     *
     * @exception SyncFailedException if a possibly recoverable error occurs.
     * @exception IOException If an IO error occurs.
     */
    void sync() throws IOException;

    /**
     * Reads up to <code>len</code> bytes of data from this file into an
     * array of bytes. This method blocks until at least one byte of input
     * is available.
     * <p>
     *
     * @param b     the buffer into which the data is read.
     * @param off   the start offset in array <code>b</code>
     *                   at which the data is written.
     * @param len   the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the file has been reached.
     * @exception IOException If the first byte cannot be read for any reason
     * other than end of file, or if the random access file has been closed, or
     * if some other I/O error occurs.
     * @exception NullPointerException If <code>b</code> is <code>null</code>.
     * @exception IndexOutOfBoundsException If <code>off</code> is negative,
     * <code>len</code> is negative, or <code>len</code> is greater than
     * <code>b.length - off</code>
     */
    int read(byte[] b, int off, int len) throws IOException;
}
