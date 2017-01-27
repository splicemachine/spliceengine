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

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import com.splicemachine.db.iapi.services.io.InputStreamUtil;

/**
 * Updatable blob stream is a wrapper stream over dvd stream
 * and LOBInputStream.
 * It detects if blob data has moved from dvd to clob control. If this happens,
 * it will update itself to point to LOBInputStream and reflect changes made to
 * the Blob after the current position of the stream.
 */
class UpdatableBlobStream extends InputStream {
    /**
     * Flag to check if it is using stream from LOBStreamControl or from DVD.
     * <code>true</code> means data is read from LOBStreamControl,
     * <code>false</code> means data is read from the DVD.
     */
    private boolean materialized;
    private InputStream stream;
    /* Current position of this stream in number of bytes. */
    private long pos;
    private final EmbedBlob blob;
    
    /**
     * Position in Blob where to stop reading unless EOF is reached first.
     */
    private final long maxPos;
    

    /**
     * Constructs UpdatableBlobStream using the the InputStream receives as the
     * parameter. The initial position is set to the <code>0</code>.
     * 
     * @param blob EmbedBlob this stream is associated with.
     * @param is InputStream this class is going to use internally.
     * @throws IOException if an I/O error occurs
     */
    UpdatableBlobStream (EmbedBlob blob, InputStream is)
            throws IOException {
        // The entire Blob has been requested, hence set length to infinity (or
        // as close as we get).
        this(blob, is, 0L, Long.MAX_VALUE);
    }
    
    /**
     * Construct an <code>UpdatableBlobStream</code> using the 
     * <code>InputStream</code> received as parameter. The initial
     * position in the stream is set to <code>pos</code> and the
     * stream is restricted to a length of <code>len</code>.
     * 
     * @param blob EmbedBlob this stream is associated with.
     * @param is InputStream this class is going to use internally.
     * @param pos initial position
     * @param len The length to which the underlying <code>InputStream</code>
     *            has to be restricted.
     * @throws IOException
     */
    UpdatableBlobStream (EmbedBlob blob, InputStream is, long pos, long len) 
            throws IOException {
        this.blob = blob;
        stream = is;
        maxPos = pos + len;
        
        //Skip to the requested position
        //inside the stream.
        if (pos > 0) {
            skip(pos);
        }
    }

    /**
     * Checks if this object is using materialized blob
     * if not it checks if the blob was materialized since
     * this stream was last access. If the blob was materialized
     * (due to one of the set methods) it gets the stream again and
     * sets the position to current read position.
     */
    private void updateIfRequired () throws IOException {
        if (materialized)
            return;
        if (blob.isMaterialized()) {
            materialized = true;
            try {
                stream = blob.getBinaryStream();
            } catch (SQLException ex) {
                throw Util.newIOException(ex);
            }
            InputStreamUtil.skipFully(stream, pos);
        }
    }

    /**
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an <code>int</code> in the range <code>0</code> to
     * <code>255</code>. If no byte is available because the end of the stream
     * has been reached, the value <code>-1</code> is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     * <p>
     * A subclass must provide an implementation of this method.
     * <p>
     * Note that this stream will reflect changes made to the underlying Blob at
     * positions equal to or larger then the current position.
     *
     * @return the next byte of data, or <code>-1</code> if the end of the
     *             stream is reached.
     * @exception IOException  if an I/O error occurs.
     * @see InputStream#read
     */
    public int read() throws IOException {
        updateIfRequired();
        
        //If the current position inside the stream has exceeded maxPos, the
        //read should return -1 signifying end of stream.
        if (pos >= maxPos) {
            return -1;
        }
        int ret = stream.read();
        if (ret >= 0)
            pos++;
        return ret;
    }

    /**
     * Reads up to <code>len</code> bytes of data from the input stream into
     * an array of bytes.  An attempt is made to read as many as
     * <code>len</code> bytes, but a smaller number may be read.
     * The number of bytes actually read is returned as an integer.
     * <p>
     * Note that this stream will reflect changes made to the underlying Blob at
     * positions equal to or larger then the current position .
     *
     * @param b     the buffer into which the data is read.
     * @param off   the start offset in array <code>b</code>
     *                   at which the data is written.
     * @param len   the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the stream has been reached.
     * @exception IOException If the first byte cannot be read for any reason
     * other than end of file, or if the input stream has been closed, or if
     * some other I/O error occurs.
     * @exception NullPointerException If <code>b</code> is <code>null</code>.
     * @exception IndexOutOfBoundsException If <code>off</code> is negative,
     * <code>len</code> is negative, or <code>len</code> is greater than
     * <code>b.length - off</code>
     * @see java.io.InputStream#read(byte[],int,int)
     */
    public int read(byte[] b, int off, int len) throws IOException {
        updateIfRequired();
        long remaining = maxPos - pos;
        // Return EOF if the maximum allowed position has been reached,
        // and we're trying to read at least one byte.
        if (remaining == 0 && len > 0) {
            return -1;
        }
        int actualLength = (int) Math.min(len, remaining);
        int retValue = stream.read(b, off, actualLength);
        if (retValue > 0)
            pos += retValue;
        return retValue;
    }

    /**
     * Reads some number of bytes from the input stream and stores them into
     * the buffer array <code>b</code>. The number of bytes actually read is
     * returned as an integer.  This method blocks until input data is
     * available, end of file is detected, or an exception is thrown.
     * <p>
     * Note that this stream will reflect changes made to the underlying Blob at
     * positions equal to or larger then the current position .
     *
     * @param b   the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or
     *             <code>-1</code> is there is no more data because the end of
     *             the stream has been reached.
     * @exception IOException  If the first byte cannot be read for any reason
     * other than the end of the file, if the input stream has been closed, or
     * if some other I/O error occurs.
     * @exception NullPointerException  if <code>b</code> is <code>null</code>.
     * @see java.io.InputStream#read(byte[])
     */
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    /**
     * Skips over and discards <code>n</code> bytes of data from this input
     * stream. The <code>skip</code> method may, for a variety of reasons, end
     * up skipping over some smaller number of bytes, possibly <code>0</code>.
     * This may result from any of a number of conditions; reaching end of file
     * before <code>n</code> bytes have been skipped is only one possibility.
     * The actual number of bytes skipped is returned.  If <code>n</code> is
     * negative, no bytes are skipped.
     * <p>
     * Note that this stream will reflect changes made to the underlying Blob at
     * positions equal to or larger then the current position .
     *
     * @param n   the number of bytes to be skipped.
     * @return the actual number of bytes skipped.
     * @exception IOException  if the stream does not support seek,
     *                      or if some other I/O error occurs.
     * @see java.io.InputStream#skip(long)
     */
    public long skip(long n) throws IOException {
        updateIfRequired();
        long retValue = stream.skip(n);
        if (retValue > 0)
            pos += retValue;
        return retValue;
    }
}
