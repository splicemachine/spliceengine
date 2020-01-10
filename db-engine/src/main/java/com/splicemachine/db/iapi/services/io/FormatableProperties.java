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

import java.util.Enumeration;
import java.util.Properties;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectInput;

/**
 * A formatable holder for a java.util.Properties.
 * Used to avoid serializing Properties.
 */
public class FormatableProperties extends Properties implements Formatable
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

	/**
	 * Niladic constructor for formatable
	 */
	public FormatableProperties() 
	{
		this(null);
	}

	/**
	 * Creates an empty property list with the specified
	 * defaults.
	 *
	 * @param defaults the defaults
	 */
	public FormatableProperties(Properties defaults)
	{
		super(defaults);
	}

	/**
		Clear the defaults from this Properties set.
		This sets the default field to null and thus
		breaks any link with the Properties set that
		was the default.
	*/
	public void clearDefaults() {
		defaults = null;
	}
	
	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write the properties out.  Step through
	 * the enumeration and write the strings out
	 * in UTF.
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeInt(size());
		for (Enumeration e = keys(); e.hasMoreElements(); )
		{
			String key = (String)e.nextElement();
			out.writeUTF(key);
			out.writeUTF(getProperty(key));
		}
	}					

	/**
	 * Read the properties from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException
	{
		int size = in.readInt();
		for (; size > 0; size--)
		{
			put(in.readUTF(), in.readUTF());
		}
	}

	public void readExternal(ArrayInputStream in)
		throws IOException
	{
		int size = in.readInt();
		for (; size > 0; size--)
		{
			put(in.readUTF(), in.readUTF());
		}
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.FORMATABLE_PROPERTIES_V01_ID; }
}
