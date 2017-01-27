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
import java.io.IOException;

/**
	Converts a stream containing the Derby stored binary form
	to one that just contains the application's data.
	Simply read and save the length information.
*/
class BinaryToRawStream
extends java.io.FilterInputStream
{
    /**
     * Length of the value represented by this stream.
     * Set to -1 if the length is unknown.
     */
    private int length;

    // used by caller to insure that parent can not be GC'd until this
    // stream is no longer being used.
    private Object          parent;

	BinaryToRawStream(InputStream in, Object parent) 
        throws IOException
	{
		super(in);

		this.parent     = parent;

		int bl = in.read();
		if (bl == -1)
			throw new java.io.EOFException();

		if ((bl & 0x80) != 0)
		{
			if (bl == 0xC0)
			{
				int v1 = in.read();
				int v2 = in.read();
				int v3 = in.read();
				int v4 = in.read();

				if (v1 == -1 || v2 == -1 || v3 == -1 || v4 == -1)
					throw new java.io.EOFException();
                length = (((v1 & 0xff) << 24) |
                          ((v2 & 0xff) << 16) |
                          ((v3 & 0xff) << 8)  |
                           (v4 & 0xff));

			}
			else if (bl == 0xA0)
			{
				// read an unsigned short
				int v1 = in.read();
				int v2 = in.read();
				if (v1 == -1 || v2 == -1)
					throw new java.io.EOFException();
                length = (((v1 & 0xff) << 8) + (v2 & 0xff));

			}
			else
			{
				length = bl & 0x1F;
			}
		}
		else
		{
			// old length in bits
			int v2 = in.read();
			int v3 = in.read();
			int v4 = in.read();
			if (v2 == -1 || v3 == -1 || v4 == -1)
				throw new java.io.EOFException();
            int lenInBits = (((bl & 0xff) << 24) | ((v2 & 0xff) << 16) | ((v3 & 0xff) << 8) | (v4 & 0xff));

			length = lenInBits / 8;
			if ((lenInBits % 8) != 0)
			    length++;
            
            // Signifies unknown length
            if (length == 0)
                length = -1;
		}
	}
    
    /**
     * Return the length of the value in thie stream in bytes.
     * If the value is unknown then -1 is returned.
     */
    int getLength()
    {
        return length;
    }
}
