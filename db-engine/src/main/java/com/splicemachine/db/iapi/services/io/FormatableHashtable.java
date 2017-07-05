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

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import java.util.Hashtable;
import java.util.Enumeration;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;


/**
 * A formatable holder for a java.util.Hashtable.
 * Used to avoid serializing Properties.
 */
public class FormatableHashtable extends Hashtable implements Formatable
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
	public FormatableHashtable() 
	{
	}


	/**
	 * Our special put method that wont barf
	 * on a null value.
	 * @see java.util.Hashtable
	 */
	public Object put(Object key, Object value)
	{
		if (value == null)
		{
			return remove(key);
		}

		if (SanityManager.DEBUG) {

		if ((value instanceof FormatableIntHolder) ||
			(value instanceof FormatableLongHolder) ||
			((value instanceof java.io.Serializable) && (!(value instanceof Formatable)) && (!(value instanceof String)))
			) {

			if (!value.getClass().isArray()) {

				// System.out.println("key " + key + " class " + value.getClass());
				//new Throwable().printStackTrace(System.out);
				//System.exit(1);
			}
		}
		}
		return super.put(key, value);
	}

	public void putInt(Object key, int value) {

		super.put(key, new FormatableIntHolder(value));
	}

	public int getInt(Object key) {

		return ((FormatableIntHolder) get(key)).getInt();
	}
	public void putLong(Object key, long value) {

		super.put(key, new FormatableLongHolder(value));
	}

	public long getLong(Object key) {

		return ((FormatableLongHolder) get(key)).getLong();
	}
	public void putBoolean(Object key, boolean value) {

		putInt(key,value ? 1 : 0);
	}

	public boolean getBoolean(Object key) {

		return getInt(key) != 0;

	}

	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write the hash table out.  Step through
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
			Object key = e.nextElement();
			out.writeObject(key);
			out.writeObject(get(key));
			
			if (SanityManager.DEBUG)
			{
				Object value = get(key);
				if (value instanceof Formatable[])
				{
					SanityManager.THROWASSERT("you should be using FormatableArrayHolder rather than writing out an array of Formatables, otherwise you will get bad behavior for null Storables.  Your class is a "+value.getClass().getName());
				}
			}
		}
	}					

	/**
	 * Read the hash table from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException, ClassNotFoundException
	{
		int size = in.readInt();
		for (; size > 0; size--)
		{
			super.put(in.readObject(), in.readObject());
		}
	}
	public void readExternal(ArrayInputStream in)
		throws IOException, ClassNotFoundException
	{
		int size = in.readInt();
		for (; size > 0; size--)
		{
			super.put(in.readObject(), in.readObject());
		}
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.FORMATABLE_HASHTABLE_V01_ID; }
}
