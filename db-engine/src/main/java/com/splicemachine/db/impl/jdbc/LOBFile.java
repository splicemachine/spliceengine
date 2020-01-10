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
package com.splicemachine.db.impl.jdbc;

import java.io.FileNotFoundException;
import java.io.IOException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.io.StorageFile;
import com.splicemachine.db.io.StorageRandomAccessFile;

/**
 * LOBFile is a wrapper over StorageRandomAccessFile. The purpose of this class
 * is to let the user of this class access StorageRandomAccessFile in plain and
 * in encrypted for without having to change code.
 */
class LOBFile {
    /** The temporary file where the contents of the LOB should be stored. */
    private final StorageFile storageFile;

    /** An object giving random access to {@link #storageFile}. */
    private final StorageRandomAccessFile randomAccessFile;

    /**
     * Constructs LOBFile.
     *
     * @param lobFile StorageFile object for which the file will be created
     * @throws FileNotFoundException if the file exists but is a directory or
     * cannot be opened
     */
    LOBFile(StorageFile lobFile) throws FileNotFoundException {
        storageFile = lobFile;
        randomAccessFile = lobFile.getRandomAccessFile("rw");
    }

    /**
     * Get the {@code StorageFile} which represents the file where the
     * contents of the LOB are stored.
     * @return a {@code StorageFile} instance
     */
    StorageFile getStorageFile() {
        return storageFile;
    }

    /**
     * Returns length of the file.
     * @return length of the file
     * @throws IOException if an I/O error occurs
     */
    long length() throws IOException {
        return randomAccessFile.length();
    }

    /**
     * Sets the file pointer to a given position.
     * @param pos new position
     * @throws IOException if an I/O error occurs
     */
    void seek(long pos) throws IOException {
        randomAccessFile.seek (pos);
    }

    /**
     * Writes one bytes into the file.
     * @param b int value of the byte
     * @throws IOException if an I/O error occurs
     * @throws StandardException it won't be thrown, it's in signature to allow
     *              subclasses to throw StandardException
     */
    void write(int b) throws IOException, StandardException {
        randomAccessFile.write (b);
    }

    /**
     * Returns the current position of the file pointer.
     * @return file pointer
     * @throws IOException if an I/O error occurs
     */
    long getFilePointer() throws IOException {
        return randomAccessFile.getFilePointer();
    }

    /**
     * Writes a segment of bytes into the file.
     * @param b byte array containing bytes to write into the file
     * @param off starting position of segment
     * @param len number of bytes to be written
     * @throws IOException if an I/O error occurs
     * @throws StandardException it won't be thrown, it's in signature to allow
     *              subclasses to throw StandardException
     */
    void write(byte[] b, int off, int len)
                                    throws IOException, StandardException {
            randomAccessFile.write (b, off, len);
    }

    /**
     * Reads one byte from file.
     * @return byte
     * @throws IOException if disk operation fails
     * @throws StandardException it won't be thrown, it's in signature to allow
     *              subclasses to throw StandardException
     */
    int readByte() throws IOException, StandardException {
        return randomAccessFile.readByte();
    }

    /**
     * Reads len number of bytes from the file starting from off position
     * in the buffer.
     * @param buff buffer
     * @param off starting position of buffer
     * @param len number of bytes
     * @return number of bytes read
     * @throws IOException if an I/O error occurs
     * @throws StandardException it won't be thrown, it's in signature to allow
     *              subclasses to throw StandardException
     */
    int read(byte[] buff, int off, int len)
                                    throws IOException, StandardException {
        return randomAccessFile.read (buff, off, len);
    }

    /**
     * Closes the file.
     * @throws IOException if an I/O error occurs
     * @throws StandardException it won't be thrown, it's in signature to allow
     *              subclasses to throw StandardException
     */
    void close() throws IOException {
        randomAccessFile.close();
    }

    /**
     * Sets the file length to a given size.
     * @param size new size
     * @throws IOException if an I/O error occurs
     * @throws StandardException it won't be thrown, it's in signature to allow
     *              subclasses to throw StandardException
     */
    void setLength(long size) throws IOException, StandardException {
        randomAccessFile.setLength (size);
    }

    /**
     * Writes a buffer completely into the file.
     * @param buf buffer to write
     * @throws IOException if an I/O error occurs
     * @throws StandardException it won't be thrown, it's in signature to allow
     *              subclasses to throw StandardException
     */
    void write(byte[] buf) throws IOException, StandardException {
        randomAccessFile.write (buf);
    }

}
