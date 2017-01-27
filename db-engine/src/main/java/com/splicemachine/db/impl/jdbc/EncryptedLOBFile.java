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

package com.splicemachine.db.impl.jdbc;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.store.raw.data.DataFactory;
import com.splicemachine.db.io.StorageFile;

/**
 * This class is a wrapper class on top of StorageRandomAccess to provide common
 * methods to write in encrypted file.
 * This class is NOT thread safe. The user class should take care
 * of synchronization if being used in multi threaded environment.
 */
class EncryptedLOBFile extends LOBFile {
    /** Block size for encryption. */
    private final int blockSize;
    /** Leftover bytes. Stored in memory until they fill one block .*/
    private final byte [] tail;
    /** Number of actual bytes in tail array. */
    private int tailSize;
    /** Current file position. */
    private long currentPos;
    /** Factory object used for encryption and decryption. */
    private final DataFactory df;

    /**
     * Constructs the EncryptedLOBFile object with encryption support.
     *
     * @param lobFile StorageFile Object for which file will be created
     * @param df data factory for encryption and decription
     * @throws FileNotFoundException if the file exists but is a directory or
     * cannot be opened
     */
    EncryptedLOBFile(StorageFile lobFile, DataFactory df)
                                                throws FileNotFoundException {
        super(lobFile);
        this.df = df;
        blockSize = df.getEncryptionBlockSize();
        tail = new byte [blockSize];
        tailSize = 0;
    }

    /**
     * Find the blocks containing the data we are interested in.
     *
     * @param pos first position we are interested in
     * @param len number of bytes of interest
     * @return byte array containing all the blocks of data the specified
     * region spans over
     */
    private byte [] getBlocks (long pos, int len)
                                        throws IOException, StandardException {
        if (len < 0)
            throw new IndexOutOfBoundsException (
                    MessageService.getTextMessage (
                        SQLState.BLOB_NONPOSITIVE_LENGTH, new Integer (len)));
        //starting position of the 1st block
        long startPos = pos - pos % blockSize;
        //end position of last block
        long endPos = (pos + len + blockSize - 1) / blockSize * blockSize;

        byte [] data = new byte [(int) (endPos - startPos)];
        super.seek (startPos);
        super.read (data, 0, data.length);
        return data;
    }

    /**
     * Returns file length.
     * @return file length
     * @throws IOException if an I/O error occurs
     */
    long length() throws IOException {
        return super.length() + tailSize;
    }

    /**
     * Returns the currrent position in the file.
     * @return current position of file pointer
     */
    long getFilePointer() {
        return currentPos;
    }

    /**
     * Sets the current file pointer to specific location.
     * @param pos new position
     * @throws IOException
     */
    void seek (long pos) throws IOException {
        long fileLength = super.length();
        if (pos > fileLength + tailSize) {
            //this should never happen
            //this exception will mean internal error most
            //probably in LOBStreamControl
            throw new IllegalArgumentException ("Internal Error");
        }
        if (pos < fileLength) {
            super.seek (pos);
        }
        currentPos = pos;
    }

    /**
     * Writes one byte into the file.
     * @param b byte value
     * @throws IOException if disk operation fails
     * @throws StandardException if error occured during encryption/decryption
     */
    void write (int b) throws IOException, StandardException {
        long length = super.length();
        if (currentPos >= length) {
            //current position is in memory
            int pos = (int) (currentPos - length);
            tail [pos] = (byte) b;
            if (pos >= tailSize) {
                tailSize = pos + 1;
            }
            if (tailSize == blockSize) {
                //we have enough data to fill one block encrypt and write
                //in file
                byte [] cypherText = new byte [blockSize];
                df.encrypt (tail, 0, tailSize, cypherText, 0, false);
                super.seek (length);
                super.write (cypherText);
                tailSize = 0;
            }
        }
        else {
            //write position is in the middle of the file
            //get the complete block in which the destination byte falls into
            byte [] cypherText = getBlocks (currentPos, 1);
            byte [] clearText = new byte [blockSize];
            //decrypt the block before updating
            df.decrypt(cypherText, 0, blockSize, clearText, 0);
            clearText [(int) (currentPos%blockSize)] = (byte) b;
            //encrypt and write back
            df.encrypt (clearText, 0, blockSize, cypherText, 0, false);
            super.seek (currentPos - currentPos % blockSize);
            super.write (cypherText);
        }
        currentPos++;
    }

    /**
     * Writes length number of bytes from buffer starting from off position.
     * @param b byte array containing bytes to be written
     * @param off starting offset of the byte array from where the
     * data should be written to the file
     * @param len number of bytes to be written
     * @throws IOException if disk operation fails
     * @throws StandardException if error occured during encryption/decryption
     */
    void write(byte[] b, int off, int len)
                                    throws IOException, StandardException {
        long fileLength = super.length();
        if (currentPos < fileLength) {
            //starting position for write is in file
            //find out if we need to update memory
            int overFlow = (int) Math.max(0L, currentPos + len - fileLength);
            long oldPos = currentPos;
            //get the block containing bytes we are going to overwrite
            byte [] cypherText = getBlocks (currentPos, len - overFlow);
            byte [] clearText = new byte [cypherText.length];
            //decrypt the data before updating
            for (int i = 0; i < cypherText.length / blockSize; i++)
                df.decrypt (cypherText, i * blockSize, blockSize, clearText,
                                                                i * blockSize);
            //update the data
            System.arraycopy (b, off, clearText, (int) (currentPos%blockSize),
                len - overFlow);
            //encrypt and write back
            for (int i = 0; i < cypherText.length / blockSize; i++)
                df.encrypt (clearText, i * blockSize, blockSize,
                                        cypherText, i * blockSize, false);
            super.seek (oldPos - oldPos % blockSize);
            super.write (cypherText);
            currentPos = oldPos + cypherText.length;
            //nothing to keep in memory.
            if (overFlow == 0)
                return;
            //adjust the value to perform rest of the writes in tail buffer
            off = off + len - overFlow;
            len = overFlow;
            //write rest of the data in memory
            currentPos = fileLength;
        }
        //starting position in array
        int pos = (int) (currentPos - fileLength);
        int finalPos = pos + len;
        if (finalPos < blockSize) {
            //updated size won't be enough to perform encryption
            System.arraycopy (b, off, tail, pos, len);
            tailSize = Math.max(tailSize, pos + len);
            currentPos += len;
            return;
        }
        //number of bytes which can be encrypted
        int encLength = finalPos - finalPos % blockSize;
        int leftOver = finalPos % blockSize;
        byte [] clearText = new byte [encLength];
        //create array to encrypt
        //copy the bytes from tail which won't be overwritten
        System.arraycopy (tail, 0, clearText, 0, pos);
        //copy remaining data into array
        System.arraycopy (b, off, clearText, pos, encLength - pos);
        byte [] cypherText = new byte [clearText.length];
        //encrypt and write
        for (int offset = 0; offset < cypherText.length ; offset += blockSize)
            df.encrypt (clearText, offset, blockSize, cypherText,
                                                        offset, false);
        super.seek (fileLength);
        super.write (cypherText);
        //copy rest of it in tail
        System.arraycopy (b, off + len - leftOver, tail, 0, leftOver);
        tailSize = leftOver;
        currentPos = tailSize + fileLength + cypherText.length;
    }

    /**
     * Write the buffer into file at current position. It overwrites the
     * data if current position is in the middle of the file and appends into
     * the file if the total length exceeds the file size.
     * @param b byte array to be written
     * @throws IOException if disk operation fails
     * @throws StandardException if error occured during encryption/decryption
     */
    void write(byte[] b) throws IOException, StandardException {
        write (b, 0, b.length);
    }

    /**
     * closes the file.
     * @throws IOException
     */
    void close() throws IOException {
        super.close();
    }

    /**
     * Reads one byte from file.
     * @return byte
     * @throws IOException if disk operation fails
     * @throws StandardException if error occured during encryption/decryption
     */
    int readByte() throws IOException, StandardException {
        long fileLength = super.length();
        if (currentPos >= fileLength + tailSize)
            throw new EOFException ();
        if (currentPos >= fileLength)
            return tail [(int) (currentPos++ - fileLength)] & 0xff;
        //get the block containing the byte we are interested in
        byte cypherText [] = getBlocks (currentPos, 1);
        byte [] clearText = new byte [cypherText.length];
        df.decrypt (cypherText, 0, cypherText.length, clearText, 0);
        return clearText [(int) (currentPos++ % blockSize)] & 0xff;
    }

    /**
     * Reads len or remaining bytes in the file (whichever is lower) bytes
     * into buff starting from off position of the buffer.
     * @param buff byte array to fill read bytes
     * @param off offset of buff where the byte will be written
     * @param len number of bytes to be read
     * @return number of bytes read
     * @throws IOException if disk operation fails
     * @throws StandardException if error occured during encryption/decryption
     */
    int read(byte[] buff, int off, int len)
                                        throws IOException, StandardException {
        long fileLength = super.length();
        if (currentPos < fileLength) {
            //starting position is in file
            //find number of bytes spilling out of file
            int overFlow = (int) Math.max(0L, currentPos + len - fileLength);
            //get all the blocks
            byte [] cypherText = getBlocks (currentPos, len - overFlow);
            byte [] tmpByte = new byte [cypherText.length];
            //decrypt
            for (int offset = 0; offset < cypherText.length; offset += blockSize) {
                df.decrypt (cypherText, offset, blockSize, tmpByte,
                                                                offset);
            }
            //copy the bytes we are interested in
            System.arraycopy (tmpByte, (int) (currentPos%blockSize), buff,
                                                        off, len - overFlow);
            if (overFlow == 0) {
                currentPos += len;
                return len;
            }
            //find out total number of bytes we can read
            int newLen = Math.min(overFlow, tailSize);
            //fill the buffer from tail
            System.arraycopy (tail, 0, buff, off + len - overFlow, newLen);
            currentPos += len - overFlow + newLen;
            return len - overFlow + newLen;
        }
        int newLen = (int) Math.min (
            tailSize - currentPos + fileLength, len);
        if (newLen == 0 && len != 0)
            return -1;

        System.arraycopy (tail, (int) (currentPos - fileLength),
                            buff, off, newLen);
        currentPos += newLen;
        return newLen;
    }

    /**
     * Sets the file length to a given size. If the new size is smaller than the
     * file length the file is truncated.
     *
     * @param size new  file size. Must be lower than file length.
     * @throws IOException if file i/o fails
     * @throws StandardException if error occured during encryption/decryption
     */
    void setLength(long size) throws IOException, StandardException {
        long fileLength = super.length();
        if (size > fileLength + tailSize) {
            //this should never happen
            //this exception will mean internal error most
            //probably in LOBStreamControl
            throw new IllegalArgumentException ("Internal Error");
        }
        if (size < fileLength) {
            byte [] block = getBlocks (size, 1);
            super.setLength (size - size % blockSize);
            df.decrypt (block, 0, blockSize, tail, 0);
            tailSize = (int) (size % blockSize);
        }
        else {
            tailSize = (int) (size - fileLength);
        }
    }
}
