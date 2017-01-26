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

package com.splicemachine.db.impl.sql.execute;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Vector;

import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

/**
 * Vector of WindowFunctionInfo objects.
 *
 * @see java.util.Vector
 *
 */
public class WindowFunctionInfoList extends Vector<WindowFunctionInfo> implements Formatable
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
	**	method.  OR, since this is something that is used
	**	in stored prepared statements, it is ok to change it
	**	if you make sure that stored prepared statements are
	**	invalidated across releases.
	**
	********************************************************/

	/**
	 * Niladic constructor for Formatable
	 */
	public WindowFunctionInfoList() {}

	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////

	/** @exception  java.io.IOException thrown on error */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		int count = size();
		out.writeInt(count);
		for (int i = 0; i < count; i++)
		{
			out.writeObject(elementAt(i));
		}
	}

	/**
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception java.io.IOException on error
	 * @exception ClassNotFoundException on error	
	 */
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		int count = in.readInt();

		ensureCapacity(count);
		for (int i = 0; i < count; i++)
		{
            WindowFunctionInfo info = (WindowFunctionInfo)in.readObject();
			addElement(info);
		}	
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.AGG_INFO_LIST_V01_ID; }

	///////////////////////////////////////////////////////////////
	//
	// OBJECT INTERFACE
	//
	///////////////////////////////////////////////////////////////
}
