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

package com.splicemachine.db.impl.services.uuid;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.Formatable;

import com.splicemachine.db.catalog.UUID;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.io.StringReader;


public class BasicUUID implements UUID, Formatable
{
	/*
	** Fields of BasicUUID
	*/
	
	public long majorId; // only using 48 bits
    public long timemillis;
    public int sequence;

	/*
	** Methods of BasicUUID
	*/

	/**
		Constructor only called by BasicUUIDFactory.
	**/
	public BasicUUID(long majorId, long timemillis, int sequence)
	{
		this.majorId = majorId;
		this.timemillis = timemillis;
		this.sequence = sequence;
	}

	/**
		Constructor only called by BasicUUIDFactory.
		Constructs a UUID from the string representation
		produced by toString.
		@see BasicUUID#toString
	**/
	public BasicUUID(String uuidstring)
	{
		StringReader sr = new StringReader(uuidstring);
		sequence = (int) readMSB(sr);

		long ltimemillis = readMSB(sr) << 32;
		ltimemillis += readMSB(sr) << 16;
		ltimemillis += readMSB(sr);
		timemillis = ltimemillis;
		majorId = readMSB(sr);
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public BasicUUID() { super(); }

	/**
		Write this out.
		@exception IOException error writing to log stream
	*/
	public void writeExternal(ObjectOutput out) throws IOException 
	{
		out.writeLong(majorId);
		out.writeLong(timemillis);
		out.writeInt(sequence);
	}

	/**
		Read this in
		@exception IOException error reading from log stream
	*/
	public void readExternal(ObjectInput in) throws IOException
	{
		majorId = in.readLong();
		timemillis = in.readLong();
		sequence = in.readInt();
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.BASIC_UUID;
	}

	private static void writeMSB(char[] data, int offset, long value, int nbytes)
    {
    	for (int i = nbytes - 1; i >= 0; i--)
		{
		   long b = (value & (255L << (8 * i))) >>> (8 * i);

		   int c = (int) ((b & 0xf0) >> 4);
		   data[offset++] = (char) (c < 10 ? c + '0' : (c - 10) + 'a');
		   c = (int) (b & 0x0f);
		   data[offset++] = (char) (c < 10 ? c + '0' : (c - 10) + 'a');
		} 
    }

    /**
		Read a long value, msb first, from its character 
		representation in the string reader, using '-' or
		end of string to delimit.
	**/
	private static long readMSB(StringReader sr)
    {
		long value = 0;

		try
		{
			int c;
			while ((c = sr.read()) != -1)
			{
				if (c == '-')
					break;
				value <<= 4;

				int nibble;
				if (c <= '9')
					nibble = c - '0';
				else if (c <= 'F')
					nibble = c - 'A' + 10;
				else
					nibble = c - 'a' + 10;
				value += nibble;
			}
		}
		catch (Exception ignored)
		{
		}

		return value;
    }

	/*
	** Methods of UUID
	*/

	/**
		Implement value equality.

	**/
	public boolean equals(Object otherObject)
	{
		if (!(otherObject instanceof BasicUUID))
			return false;

		BasicUUID other = (BasicUUID) otherObject;

		return (this.sequence == other.sequence)
			&& (this.timemillis == other.timemillis)
			&& (this.majorId == other.majorId);
	}

	/**
		Provide a hashCode which is compatible with
		the equals() method.
	**/
	public int hashCode()
	{
		long hc = majorId ^ timemillis;

		return sequence ^ ((int) (hc >> 4));
	}

	/**
		Produce a string representation of this UUID which
		can be passed to UUIDFactory.recreateUUID later on
		to reconstruct it.  The funny representation is 
		designed to (sort of) match the format of Microsoft's
		UUIDGEN utility.
	 */
	public String toString() {return stringWorkhorse( '-' );}

	/**
		Produce a string representation of this UUID which
		is suitable for use as a unique ANSI identifier.
	 */
	public String toANSIidentifier() {return "U" + stringWorkhorse( 'X' );}

	/**
	  *	Private workhorse of the string making routines.
	  *
	  *	@param	separator	Character to separate number blocks.
	  *                     Null means do not include a separator.
	  *
	  *	@return	string representation of UUID.
	  */
	public	String	stringWorkhorse( char separator )
	{
		char[] data = new char[36];

		writeMSB(data, 0, (long) sequence, 4);

		int offset = 8;
		if (separator != 0) data[offset++] = separator;

		long ltimemillis = timemillis;
		writeMSB(data, offset, (ltimemillis & 0x0000ffff00000000L) >>> 32, 2);
		offset += 4;
		if (separator != 0) data[offset++] = separator;
		writeMSB(data, offset, (ltimemillis & 0x00000000ffff0000L) >>> 16, 2);
		offset += 4;
		if (separator != 0) data[offset++] = separator;
		writeMSB(data, offset, (ltimemillis & 0x000000000000ffffL), 2);
		offset += 4;
		if (separator != 0) data[offset++] = separator;
		writeMSB(data, offset, majorId, 6);
		offset += 12;

		return new String(data, 0, offset);
	}

	/**
	  Clone this UUID.

	  @return	a copy of this UUID
	  */
	public UUID cloneMe()
	{
		return	new	BasicUUID(majorId, timemillis, sequence);
	}
}

