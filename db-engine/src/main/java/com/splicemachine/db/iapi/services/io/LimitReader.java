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

import java.io.Reader;
import java.io.IOException;

/**
	A  Reader that provides methods to limit the range that
	can be read from the reader.
*/
public final class LimitReader extends Reader implements Limit 
{
	private int remainingCharacters;
	private boolean limitInPlace;
	private	Reader	reader;

	/**
		Construct a LimitReader and call the clearLimit() method.
	*/
	public LimitReader(Reader reader) 
	{
		super();
		this.reader = reader;
		clearLimit();
	}

	public int read() throws IOException 
	{

		if (!limitInPlace)
			return reader.read();
		
		if (remainingCharacters == 0)
			return -1; // end of file

		
		int value = reader.read();
		if (value >= 0)
			remainingCharacters--;
		return value;

	}

	public int read(char c[], int off, int len) throws IOException 
	{
		if (!limitInPlace)
			return reader.read(c, off, len);

		if (remainingCharacters == 0)
			return -1;

		if (remainingCharacters < len) 
		{
			len = remainingCharacters; // end of file
		}

		len = reader.read(c, off, len);
		if (len >= 0)
			remainingCharacters -= len;
		return len;
	}

	public long skip(long count)
		throws IOException 
	{
		if (!limitInPlace)
			return reader.skip(count);

		if (remainingCharacters == 0)
			return 0; // end of file

		if (remainingCharacters < count)
			count = remainingCharacters;

		count = reader.skip(count);
		remainingCharacters -= count;
		return count;
	}

	public void close()
		throws IOException 
	{
		reader.close();
	}

	/**
		Set the limit of the stream that can be read. After this
		call up to and including length characters can be read from
        or skipped in the stream.
        Any attempt to read more than length characters will
		result in an EOFException

		@exception IOException IOException from some underlying stream
		@exception EOFException The set limit would exceed
		the available data in the stream.
	*/
	public void setLimit(int length) 
	{
		remainingCharacters = length;
		limitInPlace = true;
    }
    
    /**
     * return limit of the stream that can be read without throwing
     * EOFException
     * @return the remaining characters left to be read from the stream
     */
    public final int getLimit()
    {
        return remainingCharacters;
    }

	/**
		Clear any limit set by setLimit. After this call no limit checking
		will be made on any read until a setLimit()) call is made.

		@return the number of bytes within the limit that have not been read.
		-1 if not limit was set.
	*/
	public int clearLimit() 
	{
		int leftOver = remainingCharacters;
		limitInPlace = false;
		remainingCharacters = -1;
		return leftOver;
	}
}
