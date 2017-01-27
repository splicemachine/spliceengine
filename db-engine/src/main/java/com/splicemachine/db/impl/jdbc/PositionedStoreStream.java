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
import com.splicemachine.db.iapi.services.io.InputStreamUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.types.PositionedStream;
import com.splicemachine.db.iapi.types.Resetable;

/**
 * A wrapper-stream able to reposition the underlying store stream.
 * <p>
 * Where a user expects the underlying stream to be at a given position,
 * {@link #reposition} must be called with the expected position first. A use
 * case for this scenario is the LOB objects, where you can request a stream and
 * at the same time (this does not mean concurrently) query the LOB about its
 * length or ask to get a part of the LOB returned. Such multiplexed operations
 * must result in consistent and valid data, and to achieve this the underlying
 * store stream must be able to reposition itself.
 *
 * <em>Synchronization</em>: Access to instances of this class must be
 * externally synchronized on the connection synchronization object. There are
 * two reasons for this:
 * <ul> <li>Access to store must be single threaded.
 *      <li>This class is not thread safe, and calling the various methods from
 *          different threads concurrently can result in inconsistent position
 *          values. To avoid redundant internal synchronization, this class
 *          assumes and <b>requires</b> external synchronization (also called
 *          client-side locking).
 * </ul>
 * @see EmbedConnection#getConnectionSynchronization
 */
//@NotThreadSafe
public class PositionedStoreStream
    extends InputStream
    implements PositionedStream, Resetable {

    /** Underlying store stream serving bytes. */
    //@GuardedBy("EmbedConnection.getConnectionSynchronization()")
    private final InputStream stream;
    /**
     * Position of the underlying store stream.
     * Note that the position is maintained by this class, not the underlying
     * store stream itself.
     * <em>Future improvement</em>: Add this functionality to the underlying
     * store stream itself to avoid another level in the stream stack.
     */
    //@GuardedBy("EmbedConnection.getConnectionSynchronization()")
    private long pos = 0L;

    /**
     * Creates a positioned store stream on top of the specified resettable
     * stream.
     * <p>
     * Upon creation, the underlying stream is initiated and reset to make
     * sure the states of the streams are in sync with each other.
     *
     * @param in a {@link Resetable}-stream
     */
    public PositionedStoreStream(InputStream in)
            throws IOException, StandardException {
        this.stream = in;
        // We need to know the stream is in a consistent state.
        ((Resetable)in).initStream();
        ((Resetable)in).resetStream();
    }

    /**
     * Reads a number of bytes from the underlying stream and stores them in the
     * specified byte array.
     *
     * @return The actual number of bytes read, or -1 if the end of the stream
     *      is reached.
     * @throws IOException if an I/O error occurs
     */
    public int read(byte[] b)
            throws IOException {
        return read(b, 0, b.length);
    }

    /**
     * Reads a number of bytes from the underlying stream and stores them in the
     * specified byte array at the specified offset.
     *
     * @return The actual number of bytes read, or -1 if the end of the stream
     *      is reached.
     * @throws IOException if an I/O error occurs
     */
    public int read(byte[] b, int off, int len)
            throws IOException {
        int ret = this.stream.read(b, off, len);
        if (ret > -1) {
            this.pos += ret;
        }
        return ret;
    }

    /**
     * Reads a single byte from the underlying stream.
     *
     * @return The next byte of data, or -1 if the end of the stream is reached.
     * @throws IOException if an I/O error occurs
     */
    public int read()
            throws IOException {
        int ret = this.stream.read();
        if (ret > -1) {
            this.pos++;
        }
        return ret;
    }

    /**
     * Skips up to the specified number of bytes from the underlying stream.
     *
     * @return The actual number of bytes skipped.
     * @throws IOException if an I/O error occurs
     */
    public long skip(long toSkip)
            throws IOException {
        long ret = this.stream.skip(toSkip);
        this.pos += ret;
        return ret;
    }

    /**
     * Resets the resettable stream.
     *
     * @throws IOException
     * @throws StandardException if resetting the stream in store fails
     * @see Resetable#resetStream
     */
    public void resetStream()
            throws IOException, StandardException {
        ((Resetable)this.stream).resetStream();
        this.pos = 0L;
    }

    /**
     * Initialize the resettable stream for use.
     *
     * @throws StandardException if initializing the store in stream fails
     * @see Resetable#initStream
     */
    public void initStream()
            throws StandardException {
        ((Resetable)this.stream).initStream();
    }

    /**
     * Closes the resettable stream.
     *
     * @see Resetable#closeStream
     */
    public void closeStream() {
        ((Resetable)this.stream).closeStream();
    }

    /**
     * Repositions the underlying store stream to the requested position.
     * <p>
     * Repositioning is required because there can be several uses of the store
     * stream, which changes the position of it. If a class is dependent on the
     * underlying stream not changing its position, it must call reposition with
     * the position it expects before using the stream again.
     * <p>
     * If the repositioning fails because the stream is exhausted, most likely
     * because of an invalid position specified by the user, the stream is
     * reset to position zero and the {@code EOFException} is rethrown.
     *
     * @throws EOFException if the stream is exhausted before the requested
     *      position is reached
     * @throws IOException if reading from the store stream fails
     * @throws StandardException if resetting the store in stream fails, or
     *      some other exception happens in store
     * @see #getPosition
     */
    public void reposition(final long requestedPos)
            throws IOException, StandardException {
        if (SanityManager.DEBUG) {
            if (requestedPos < 0) {
                SanityManager.THROWASSERT("Negative position: " + requestedPos);
            }
        }
        if (this.pos > requestedPos) {
            // Reset stream to reposition from start.
            resetStream();
        }
        if (this.pos < requestedPos) {
            try {
                InputStreamUtil.skipFully(stream, requestedPos - pos);
            } catch (EOFException eofe) {
                // A position after the end of the stream was requested.
                // To recover, and for consistency, reset to position zero.
                resetStream();
                throw eofe;
            }
            // Operation successful, update position.
            this.pos = requestedPos;
        }
    }

    /**
     * Returns the current position of the underlying store stream.
     *
     * @return Current byte position of the store stream.
     */
    public long getPosition() {
        return this.pos;
    }

    public InputStream asInputStream() {
        return this;
    }

} // End class PositionedStoreStream
