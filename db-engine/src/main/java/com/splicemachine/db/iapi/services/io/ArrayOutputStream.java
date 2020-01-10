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

package com.splicemachine.db.iapi.services.io;

import java.io.IOException;
import java.io.EOFException;
import java.io.OutputStream;

public class ArrayOutputStream extends OutputStream implements Limit {

	private byte[] pageData;

	private int		start;
	private int		end;		// exclusive
	private int		position;

	public ArrayOutputStream() {
		super();
	}

	public ArrayOutputStream(byte[] data) {
		super();
		setData(data);
	}

	public void setData(byte[] data) {
		pageData = data;
		start = 0;
		if (data != null)
			end = data.length;
		else
			end = 0;
		position = 0;

	}

	/*
	** Methods of OutputStream
	*/

	public void write(int b) throws IOException {
		if (position >= end)
			throw new EOFException();

		pageData[position++] = (byte) b;

	}

	public void write(byte b[], int off, int len) throws IOException {

		if ((position + len) > end)
			throw new EOFException();

		System.arraycopy(b, off, pageData, position, len);
		position += len;
	}

	/*
	** Methods of LengthOutputStream
	*/

	public int getPosition() {
		return position;
	}

	/**
		Set the position of the stream pointer.
	*/
	public void setPosition(int newPosition)
		throws IOException {
		if ((newPosition < start) || (newPosition > end))
			throw new EOFException();

		position = newPosition;
	}

	public void setLimit(int length) throws IOException {

		if (length < 0) {
			throw new EOFException();
		}

		if ((position + length) > end) {
			throw new EOFException();
		}

		start = position;
		end = position + length;

    }

	public int clearLimit() {

		int unwritten = end - position;

		end = pageData.length;

		return unwritten;
	}
}
