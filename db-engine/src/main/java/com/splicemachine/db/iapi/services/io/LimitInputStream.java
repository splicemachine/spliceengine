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

import java.io.InputStream;
import java.io.FilterInputStream;
import java.io.IOException;

/**
	An abstract InputStream that provides abstract methods to limit the range that
	can be read from the stream.
*/
public class LimitInputStream extends FilterInputStream implements Limit {

	protected int remainingBytes;
	protected boolean limitInPlace;

	/**
		Construct a LimitInputStream and call the clearLimit() method.
	*/
	public LimitInputStream(InputStream in) {
		super(in);
		clearLimit();
	}

	public int read() throws IOException {

		if (!limitInPlace)
			return super.read();
		
		if (remainingBytes == 0)
			return -1; // end of file

		
		int value = super.read();
		if (value >= 0)
			remainingBytes--;
		return value;

	}

	public int read(byte b[], int off, int len) throws IOException {

		if (!limitInPlace)
			return super.read(b, off, len);


		if (remainingBytes == 0)
			return -1;

		if (remainingBytes < len) {
			len = remainingBytes; // end of file
		}

		len = super.read(b, off, len);
		if (len > 0)
			remainingBytes -= len;

		return len;
	}

	public long skip(long count)  throws IOException {
		if (!limitInPlace)
			return super.skip(count);

		if (remainingBytes == 0)
			return 0; // end of file

		if (remainingBytes < count)
			count = remainingBytes;

		count = super.skip(count);
		remainingBytes -= count;
		return count;
	}

	public int available() throws IOException {

		if (!limitInPlace)
			return super.available();

		if (remainingBytes == 0)
			return 0; // end of file

		int actualLeft = super.available();

		if (remainingBytes < actualLeft)
			return remainingBytes;
		

		return actualLeft;
	}


	/**
		Set the limit of the stream that can be read. After this
		call up to and including length bytes can be read from or skipped in
		the stream. Any attempt to read more than length bytes will
		result in an EOFException

		@exception IOException IOException from some underlying stream
		@exception EOFException The set limit would exceed
		the available data in the stream.
	*/
	public void setLimit(int length) {
		remainingBytes = length;
		limitInPlace = true;
		return;
	}

	/**
		Clear any limit set by setLimit. After this call no limit checking
		will be made on any read until a setLimit()) call is made.

		@return the number of bytes within the limit that have not been read.
		-1 if no limit was set.
	*/
	public int clearLimit() {
		int leftOver = remainingBytes;
		limitInPlace = false;
		remainingBytes = -1;
		return leftOver;
	}

	public void setInput(InputStream in) {
		this.in = in;
	}

    /**
     * This stream doesn't support mark/reset, independent of whether the
     * underlying stream does so or not.
     * <p>
     * The reason for not supporting mark/reset, is that it is hard to combine
     * with the limit functionality without always keeping track of the number
     * of bytes read.
     *
     * @return {@code false}
     */
    public boolean markSupported() {
        return false;
    }
}
