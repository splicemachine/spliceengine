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

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * A formatable holder for an long.
 */
public class FormatableLongHolder implements Formatable
{

	// the int
	private long theLong;
	
	/**
	 * Niladic constructor for formatable
	 */
	public FormatableLongHolder() 
	{
	}

	/**
	 * Construct a FormatableLongHolder using the input integer.
	 *
	 * @param theLong the long to hold
	 */
	public FormatableLongHolder(long theLong)
	{
		this.theLong = theLong;
	}

	/**
	 * Set the held long to the input int.
	 *
	 * @param theLong the int to hold
	 */
	public void setLong(int theLong)
	{
		this.theLong = theLong;
	}

	/**
	 * Get the held int.
	 *
	 * @return	The held int.
	 */
	public long getLong()
	{
		return theLong;
	}

	/**
	 * Create and return an array of FormatableLongHolders
	 * given an array of ints.
	 *
	 * @param theLongs	The array of longs
	 *
	 * @return	An array of FormatableLongHolders
	 */
	public static FormatableLongHolder[] getFormatableLongHolders(long[] theLongs)
	{
		if (theLongs == null)
		{
			return null;
		}

		FormatableLongHolder[] flhArray = new FormatableLongHolder[theLongs.length];

		for (int index = 0; index < theLongs.length; index++)
		{
			flhArray[index] = new FormatableLongHolder(theLongs[index]);
		}
		return flhArray;
	}

	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write this formatable out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeLong(theLong);
	}

	/**
	 * Read this formatable from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException
	{
		theLong = in.readLong();
	}
	public void readExternal(ArrayInputStream in)
		throws IOException
	{
		theLong = in.readLong();
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.FORMATABLE_LONG_HOLDER_V01_ID; }
}
