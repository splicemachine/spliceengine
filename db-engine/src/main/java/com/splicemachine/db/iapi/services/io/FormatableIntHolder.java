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
 * A formatable holder for an int.
 */
public class FormatableIntHolder implements Formatable
{
	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	// the int
	private int theInt;
	
	/**
	 * Niladic constructor for formatable
	 */
	public FormatableIntHolder() 
	{
	}

	/**
	 * Construct a FormatableIntHolder using the input int.
	 *
	 * @param theInt the int to hold
	 */
	public FormatableIntHolder(int theInt)
	{
		this.theInt = theInt;
	}

	/**
	 * Set the held int to the input int.
	 *
	 * @param theInt the int to hold
	 */
	public void setInt(int theInt)
	{
		this.theInt = theInt;
	}

	/**
	 * Get the held int.
	 *
	 * @return	The held int.
	 */
	public int getInt()
	{
		return theInt;
	}

	/**
	 * Create and return an array of FormatableIntHolders
	 * given an array of ints.
	 *
	 * @param theInts	The array of ints
	 *
	 * @return	An array of FormatableIntHolders
	 */
	public static FormatableIntHolder[] getFormatableIntHolders(int[] theInts)
	{
		if (theInts == null)
		{
			return null;
		}

		FormatableIntHolder[] fihArray = new FormatableIntHolder[theInts.length];

		for (int index = 0; index < theInts.length; index++)
		{
			fihArray[index] = new FormatableIntHolder(theInts[index]);
		}
		return fihArray;
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
		out.writeInt(theInt);
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
		theInt = in.readInt();
	}
	public void readExternal(ArrayInputStream in)
		throws IOException
	{
		theInt = in.readInt();
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.FORMATABLE_INT_HOLDER_V01_ID; }
}
