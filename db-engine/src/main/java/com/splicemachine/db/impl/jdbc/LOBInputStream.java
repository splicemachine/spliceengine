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
import java.io.IOException;
import java.io.InputStream;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.types.PositionedStream;
import com.splicemachine.db.iapi.error.ExceptionUtil;

/**
 * This input stream is built on top of {@link LOBStreamControl}.
 * <p>
 * All the read methods are routed to {@link LOBStreamControl}.
 */

public class LOBInputStream
    extends InputStream
    implements PositionedStream {

    private boolean closed;
    private final LOBStreamControl control;
    private long pos;
    private long updateCount;

    LOBInputStream(LOBStreamControl control, long position) {
        closed = false;
        this.control = control;
        pos = position;
        updateCount = control.getUpdateCount ();
    }

    /**
     * Reads up to <code>len</code> bytes of data from the input stream into
     * an array of bytes.  An attempt is made to read as many as
     * <code>len</code> bytes, but a smaller number may be read.
     * The number of bytes actually read is returned as an integer.
     *
     * <p> This method blocks until input data is available, end of file is
     * detected, or an exception is thrown.
     *
     * <p> If <code>b</code> is <code>null</code>, a
     * <code>NullPointerException</code> is thrown.
     *
     * <p> If <code>off</code> is negative, or <code>len</code> is negative, or
     * <code>off+len</code> is greater than the length of the array
     * <code>b</code>, then an <code>IndexOutOfBoundsException</code> is
     * thrown.
     *
     * <p> If <code>len</code> is zero, then no bytes are read and
     * <code>0</code> is returned; otherwise, there is an attempt to read at
     * least one byte. If no byte is available because the stream is at end of
     * file, the value <code>-1</code> is returned; otherwise, at least one
     * byte is read and stored into <code>b</code>.
     *
     * <p> The first byte read is stored into element <code>b[off]</code>, the
     * next one into <code>b[off+1]</code>, and so on. The number of bytes read
     * is, at most, equal to <code>len</code>. Let <i>k</i> be the number of
     * bytes actually read; these bytes will be stored in elements
     * <code>b[off]</code> through <code>b[off+</code><i>k</i><code>-1]</code>,
     * leaving elements <code>b[off+</code><i>k</i><code>]</code> through
     * <code>b[off+len-1]</code> unaffected.
     *
     * <p> In every case, elements <code>b[0]</code> through
     * <code>b[off]</code> and elements <code>b[off+len]</code> through
     * <code>b[b.length-1]</code> are unaffected.
     *
     * <p> If the first byte cannot be read for any reason other than end of
     * file, then an <code>IOException</code> is thrown. In particular, an
     * <code>IOException</code> is thrown if the input stream has been closed.
     *
     * <p> The <code>read(b,</code> <code>off,</code> <code>len)</code> method
     * for class <code>InputStream</code> simply calls the method
     * <code>read()</code> repeatedly. If the first such call results in an
     * <code>IOException</code>, that exception is returned from the call to
     * the <code>read(b,</code> <code>off,</code> <code>len)</code> method.  If
     * any subsequent call to <code>read()</code> results in a
     * <code>IOException</code>, the exception is caught and treated as if it
     * were end of file; the bytes read up to that point are stored into
     * <code>b</code> and the number of bytes read before the exception
     * occurred is returned.  Subclasses are encouraged to provide a more
     * efficient implementation of this method.
     *
     * @param b     the buffer into which the data is read.
     * @param off   the start offset in array <code>b</code>
     *                   at which the data is written.
     * @param len   the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the stream has been reached.
     * @exception IOException  if an I/O error occurs.
     * @exception NullPointerException  if <code>b</code> is <code>null</code>.
     * @see java.io.InputStream#read()
     */
    public int read(byte[] b, int off, int len) throws IOException {
        if (closed)
            throw new IOException (
                   MessageService.getTextMessage(MessageId.OBJECT_CLOSED));
        try {
            int ret = control.read(b, off, len, pos);
            if (ret != -1) {
                pos += ret;
                return ret;
            }
            return -1;
        } catch (StandardException se) {
            String state = se.getSQLState();
            if (state.equals(ExceptionUtil.getSQLStateFromIdentifier(
                                        SQLState.BLOB_POSITION_TOO_LARGE))) {
                return -1;
            } else if (state.equals(ExceptionUtil.getSQLStateFromIdentifier(
                                            SQLState.BLOB_INVALID_OFFSET))) {
                throw new ArrayIndexOutOfBoundsException(se.getMessage());
            } else {
                throw Util.newIOException(se);
            }
        }
    }

    /**
     * Closes this input stream and releases any system resources associated
     * with the stream.
     *
     * <p> The <code>close</code> method of <code>InputStream</code> does
     * nothing.
     *
     * @exception IOException  if an I/O error occurs.
     */
    public void close() throws IOException {
        closed = true;
    }

    /**
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an <code>int</code> in the range <code>0</code> to
     * <code>255</code>. If no byte is available because the end of the stream
     * has been reached, the value <code>-1</code> is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     *
     * <p> A subclass must provide an implementation of this method.
     *
     * @return the next byte of data, or <code>-1</code> if the end of the
     *             stream is reached.
     * @exception IOException  if an I/O error occurs.
     */
    public int read() throws IOException {
        if (closed)
            throw new IOException (
                   MessageService.getTextMessage(MessageId.OBJECT_CLOSED));
        try {
            int ret = control.read(pos);
            if (ret != -1)
                pos += 1;
            return ret;
        } catch (StandardException se) {
            throw Util.newIOException(se);
        }
    }

    /**
     * Checks if underlying StreamControl has been updated.
     * @return if stream is modified since created
     */
    boolean isObsolete () {
        return updateCount != control.getUpdateCount();
    }
    
    /**
     * Reinitializes the stream and sets the current pointer to zero.
     */
    void reInitialize () {
        updateCount = control.getUpdateCount();
        pos = 0;
    }
    
    /**
     * Returns size of stream in bytes.
     * @return size of stream.
     */
    long length () throws IOException {
        return control.getLength();
    }

    // Implementation of the PositionedStream interface:
    //   - asInputStream
    //   - getPosition
    //   - reposition

    public InputStream asInputStream() {
        return this;
    }

    /**
     * Returns the current byte position.
     *
     * @return The current byte position.
     */
    public long getPosition() {
        return pos;
    }

    /**
     * Repositions the stream to the requested byte position.
     *
     * @param requestedPos the requested position, starting at {@code 0}
     * @throws EOFException if the requested position is larger than the length
     * @throws IOException if obtaining the stream length fails
     */
    public void reposition(long requestedPos)
            throws IOException{
        if (SanityManager.DEBUG) {
            if (requestedPos < 0) {
                SanityManager.THROWASSERT("Negative position: " + requestedPos);
            }
        }
        if (requestedPos > length()) {
            pos = 0;
            throw new EOFException();
        }
        pos = requestedPos;
    }
}
