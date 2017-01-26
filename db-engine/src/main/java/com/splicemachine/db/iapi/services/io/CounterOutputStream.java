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

import java.io.OutputStream;
import java.io.IOException;
import java.io.EOFException;

/**
	An OutputStream that simply provides methods to count the number
	of bytes written to an underlying stream.
*/

public class CounterOutputStream extends OutputStream implements Limit {

	protected OutputStream out;
	private int count;
	private int limit;

	/**
		Create a CounterOutputStream that will discard any bytes
		written but still coutn them and call its reset method
		so that the count is intially zero.
	*/
	public CounterOutputStream() {
        limit = -1;
	}

	public void setOutputStream(OutputStream out) {
		this.out = out;
		setLimit(-1);
	}

	/**
		Get count of bytes written to the stream since the last
		reset() call.
	*/
	public int getCount() {
		return count;
	}

	/**
		Set a limit at which an exception will be thrown. This allows callers
		to count the number of bytes up to some point, without having to complete
		the count. E.g. a caller may only want to see if some object will write out
		over 4096 bytes, without waiting for all 200,000 bytes of the object to be written.
		<BR>
		If the passed in limit is 0 or negative then the stream will count bytes without
		throwing an exception.

		@see EOFException
	*/
	public void setLimit(int limit) {

		count = 0;

		this.limit = limit;

		return;
	}

	public int clearLimit() {

		int unused = limit - count;
		limit = 0;

		return unused;
	}

	/*
	** Methods of OutputStream
	*/

	/**
		Add 1 to the count.

		@see OutputStream#write
	*/
	public  void write(int b) throws IOException {
		
		if ((limit >= 0) && ((count + 1) > limit)) {
			throw new EOFException();
		}

		if (out != null) out.write(b);
		count++;
	}

	/**
		Add len to the count, discard the data.

		@see OutputStream#write
	*/
	public void write(byte b[], int off, int len) throws IOException {

		if ((limit >= 0) && ((count + len) > limit)) {
			throw new EOFException();
		}

		if (out != null) out.write(b, off, len);
		count += len;
	}
}
