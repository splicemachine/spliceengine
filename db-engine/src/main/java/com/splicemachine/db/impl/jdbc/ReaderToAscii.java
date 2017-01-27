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

import java.io.InputStream;
import java.io.Reader;
import java.io.IOException;

/**
	ReaderToAscii converts Reader (with characters) to a stream of ASCII characters.
*/
public final class ReaderToAscii extends InputStream
{

	private final Reader data;
	private char[]	conv;
	private boolean closed;

	public ReaderToAscii(Reader data) 
	{
		this.data = data;
		if (!(data instanceof UTF8Reader))
			conv = new char[256];
	}

	public int read() throws IOException
	{
		if (closed)
			throw new IOException();

		int c = data.read();
		if (c == -1)
			return -1;

		if (c <= 255)
			return c & 0xFF;
		else
			return '?'; // Question mark - out of range character.
	}

	public int read(byte[] buf, int off, int len) throws IOException
	{
		if (closed)
			throw new IOException();

		if (data instanceof UTF8Reader) {

			return ((UTF8Reader) data).readAsciiInto(buf, off, len);
		}

		if (len > conv.length)
			len = conv.length;

		len = data.read(conv, 0, len);
		if (len == -1)
			return -1;

		for (int i = 0; i < len; i++) {
			char c = conv[i];

			byte cb;
			if (c <= 255)
				cb = (byte) c;
			else
				cb = (byte) '?'; // Question mark - out of range character.
				
			buf[off++] = cb;
		}

		return len;
	}

	public long skip(long len) throws IOException {
		if (closed)
			throw new IOException();

		return data.skip(len);
	}

	public void close() throws IOException
	{
		if (!closed) {
			closed = true;
			data.close();
		}
	}
}
