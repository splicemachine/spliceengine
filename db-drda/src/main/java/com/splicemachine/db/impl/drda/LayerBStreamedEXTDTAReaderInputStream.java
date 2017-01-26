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
package com.splicemachine.db.impl.drda;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Implementation of InputStream which get EXTDTA from the DDMReader.
 * This class can be used to stream LOBs from Network client to the
 * Network server.
 *
 * Furthermore, this class is used when layer B streaming is carried out and
 * expects corresponding DDMReader start layer B streaming 
 * when the object of this class is instantiated.
 *
 */
final class LayerBStreamedEXTDTAReaderInputStream extends EXTDTAReaderInputStream
{
    /**
     * Constructor
     * @param reader The reader to get data from
     * @param readStatusByte whether or not to read the trailing Derby-specific
     *      EXTDTA stream status byte
     * @exception DRDAProtocolException if thrown while initializing current 
     *                                  buffer.
     */
    LayerBStreamedEXTDTAReaderInputStream(final DDMReader reader,
                                          boolean readStatusByte)
        throws DRDAProtocolException
    {
        super(true, readStatusByte);
        this.reader = reader;
        this.currentBuffer = 
            reader.readLOBInitStream();
    }

    /**
     * Reads the next byte of data from the input stream.
     * 
     * <p> This subclass of InputStream implements this method by reading
     * the next byte from the current buffer. If there is more data,
     * it will be requested a new buffer from the DDMReader.
     *
     * @return     the next byte of data, or <code>-1</code> if the end of the
     *             stream is reached.
     * @exception  IOException  if an I/O error occurs.
     * @see        java.io.InputStream#read()
     */
    public final int read() 
            throws IOException {
        // Reuse the other read method for simplicity.
        byte[] b = new byte[1];
        int read = read(b);
        return (read == 1 ? b[0] : -1);
    }
    
    /**
     * Reads up to <code>len</code> bytes of data from the input stream into
     * an array of bytes.  An attempt is made to read as many as
     * <code>len</code> bytes, but a smaller number may be read, possibly
     * zero. The number of bytes actually read is returned as an integer.
     *
     * This subclass implements this method by calling this method on the 
     * current buffer, which is an instance of ByteArrayInputStream. If the
     * current buffer does not have any data, it will be requested a new
     * buffer from the DDMReader.
     *
     * @param      b     the buffer into which the data is read.
     * @param      off   the start offset in array <code>b</code>
     *                   at which the data is written.
     * @param      len   the maximum number of bytes to read.
     * @return     the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the stream has been reached.
     * @exception  IOException  if an I/O error occurs.
     * @exception  NullPointerException  if <code>b</code> is <code>null</code>.
     * @see        java.io.InputStream#read(byte[], int, int)
     */
    public final int read(final byte[] b,
                          int off,
                          int len) 
        throws IOException
    {
        if (currentBuffer == null) {
            return -1;
        }

        // WARNING: We are relying on ByteArrayInputStream.available below.
        //          Replacing the stream class with another stream class may
        //          not give expected results.

        int val;
        if (reader.doingLayerBStreaming()) {
            // Simple read, we will either read part of the current buffer or
            // all of it. We know there is at least one more byte on the wire.
            val = currentBuffer.read(b, off, len);
            if (currentBuffer.available() == 0) {
                currentBuffer = reader.readLOBContinuationStream();
            }
        } else if (readStatusByte) {
            // Reading from the last buffer, make sure we handle the Derby-
            // specific status byte and that we don't return it to the user.
            int maxToRead = currentBuffer.available() -1;
            val = currentBuffer.read(b, off, Math.min(maxToRead, len));
            if (maxToRead == 0) {
                // Only status byte left.
                checkStatus(currentBuffer.read());
                val = -1;
                currentBuffer = null;
            } else if (maxToRead == val) {
                checkStatus(currentBuffer.read());
                currentBuffer = null;
            }
        } else {
            // Reading from the last buffer, no Derby-specific status byte sent.
            val = currentBuffer.read(b, off, len);
            if (currentBuffer.available() == 0) {
                currentBuffer = null;
            }
        }

        return val;
    }

    /**
     * Returns the number of bytes that can be read (or skipped over) from
     * this input stream without blocking by the next caller of a method for
     * this input stream.  
     *
     * <p> This subclass implements this method by calling available on 
     *     the current buffer, which is a ByteInputStreamReader.
     *
     * @return     the number of bytes that can be read from this input stream
     *             without blocking.     
     */
    public final int available() {
        int avail = 0;
        if (currentBuffer != null) {
            avail = currentBuffer.available();
            if (readStatusByte && !reader.doingLayerBStreaming()) {
                avail--;
            }
        }
        return avail;
    }

    
    protected void onClientSideStreamingError() {
        // Clean state and return -1 on subsequent calls.
        // The status byte is the last byte, so no need to drain the source.
        currentBuffer = null;
    }
    
    /** DDMReader. Used to get more data. */
    private final DDMReader reader;
    
    /** Current data buffer */
    private ByteArrayInputStream currentBuffer;

}
