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

package com.splicemachine.db.iapi.services.io;

import java.io.*;

/**
	Utility methods for InputStream that are stand-ins for
	a small subset of DataInput methods. This avoids pushing
	a DataInputStream just to get this functionality.
*/
public final class InputStreamUtil {
    private static final int SKIP_FRAGMENT_SIZE = Integer.MAX_VALUE;

	/**
		Read an unsigned byte from an InputStream, throwing an EOFException
		if the end of the input is reached.

		@exception IOException if an I/O error occurs.
		@exception EOFException if the end of the stream is reached

		@see DataInput#readUnsignedByte
	
	*/
	public static int readUnsignedByte(InputStream in) throws IOException {
		int b = in.read();
		if (b < 0)
			throw new EOFException();

		return b;
	}

	/**
		Read a number of bytes into an array.

		@exception IOException if an I/O error occurs.
		@exception EOFException if the end of the stream is reached

		@see DataInput#readFully

	*/
	public static void readFully(InputStream in, byte b[],
                                 int offset,
                                 int len) throws IOException
	{
		do {
			int bytesRead = in.read(b, offset, len);
			if (bytesRead < 0)
				throw new EOFException();
			len -= bytesRead;
			offset += bytesRead;
		} while (len != 0);
	}


	/**
		Read a number of bytes into an array.
        Keep reading in a loop until len bytes are read or EOF is reached or
        an exception is thrown. Return the number of bytes read.
        (InputStream.read(byte[],int,int) does not guarantee to read len bytes
         even if it can do so without reaching EOF or raising an exception.)

		@exception IOException if an I/O error occurs.
	*/
	public static int readLoop(InputStream in,
                                byte b[],
                                int offset,
                                int len)
        throws IOException
	{
        int firstOffset = offset;
		do {
			int bytesRead = in.read(b, offset, len);
			if (bytesRead <= 0)
                break;
			len -= bytesRead;
			offset += bytesRead;
		} while (len != 0);
        return offset - firstOffset;
	}

    /**
     * Skips until EOF, returns number of bytes skipped.
     * @param is
     *      InputStream to be skipped.
     * @return
     *      number of bytes skipped in fact.
     * @throws IOException
     *      if IOException occurs. It doesn't contain EOFException.
     * @throws NullPointerException
     *      if the param 'is' equals null.
     */
    public static long skipUntilEOF(InputStream is) throws IOException {
        if(is == null)
            throw new NullPointerException();

        long bytes = 0;
        while(true){
            long r = skipPersistent(is, SKIP_FRAGMENT_SIZE);
            bytes += r;
            if(r < SKIP_FRAGMENT_SIZE)
                return bytes;
        }
    }

    /**
     * Skips requested number of bytes,
     * throws EOFException if there is too few bytes in the stream.
     * @param is
     *      InputStream to be skipped.
     * @param skippedBytes
     *      number of bytes to skip. if skippedBytes <= zero, do nothing.
     * @throws EOFException
     *      if EOF meets before requested number of bytes are skipped.
     * @throws IOException
     *      if IOException occurs. It doesn't contain EOFException.
     * @throws NullPointerException
     *      if the param 'is' equals null.
     */
    public static void skipFully(InputStream is, long skippedBytes)
    throws IOException {
        if(is == null)
            throw new NullPointerException();

        if(skippedBytes <= 0)
            return;

        long bytes = skipPersistent(is, skippedBytes);

        if(bytes < skippedBytes)
            throw new EOFException();
    }

    /**
     * Tries harder to skip the requested number of bytes.
     * <p>
     * Note that even if the method fails to skip the requested number of bytes,
     * it will not throw an exception. If this happens, the caller can be sure
     * that end-of-stream has been reached.
     *
     * @param in byte stream
     * @param bytesToSkip the number of bytes to skip
     * @return The number of bytes skipped.
     * @throws IOException if reading from the stream fails
     */
    public static final long skipPersistent(InputStream in, long bytesToSkip)
    throws IOException {
        long skipped = 0;
        while (skipped < bytesToSkip) {
            long skippedNow = in.skip(bytesToSkip - skipped);
            if (skippedNow == 0) {
                if (in.read() == -1) {
                    // EOF, return what we have and leave it up to caller to
                    // decide what to do about it.
                    break;
                } else {
                    skippedNow = 1; // Added to count below.
                }
            }
            skipped += skippedNow;
        }
        return skipped;
    }
}
