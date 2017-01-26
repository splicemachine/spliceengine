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

package com.splicemachine.db.iapi.services.classfile;

import java.io.IOException;

import java.util.Hashtable;
import java.util.Vector;



class MemberTable {
	protected Vector entries;
	private Hashtable hashtable;
	private MemberTableHash	mutableMTH = null;

	public MemberTable(int count) {
		entries = new Vector(count);
		hashtable = new Hashtable((count > 50) ? count : 50);
		mutableMTH = new MemberTableHash(null, null);
	}

	void addEntry(ClassMember item) {
		MemberTableHash mth= new MemberTableHash(
									item.getName(), 
									item.getDescriptor(),
									entries.size());
		/* Add to the Vector */
		entries.add(item);

		/* Add to the Hashtable */
		hashtable.put(mth, mth);
	}

	ClassMember find(String name, String descriptor) {

		/* Set up the mutable MTH for the search */
		mutableMTH.name = name;
		mutableMTH.descriptor = descriptor;
		mutableMTH.setHashCode();

		/* search the hash table */
		MemberTableHash mth = (MemberTableHash) hashtable.get(mutableMTH);
		if (mth == null)
		{
			return null;
		}

		return (ClassMember) entries.get(mth.index);
	}

	void put(ClassFormatOutput out) throws IOException {

		Vector lentries = entries;
		int count = lentries.size();
		for (int i = 0; i < count; i++) {
			((ClassMember) lentries.get(i)).put(out);
		}
	}

	int size() {
		return entries.size();
	}

	int classFileSize() {
		int size = 0;

		Vector lentries = entries;
		int count = lentries.size();
		for (int i = 0; i < count; i++) {
			size += ((ClassMember) lentries.get(i)).classFileSize();
		}

		return size;
	}
}

class MemberTableHash 
{
	String name;
	String descriptor;
	int	   index;
	int	   hashCode;
	
	MemberTableHash(String name, String descriptor, int index)
	{
		this.name = name;
		this.descriptor = descriptor;
		this.index = index;
		/* Only set hashCode if both name and descriptor are non-null */
		if (name != null && descriptor != null)
		{
			setHashCode();
		}
	}

	MemberTableHash(String name, String descriptor)
	{
		this(name, descriptor, -1);
	}

	void setHashCode()
	{
		hashCode = name.hashCode() + descriptor.hashCode();
	}

	public boolean equals(Object other)
	{
		MemberTableHash mth = (MemberTableHash) other;

		if (other == null)
		{
			return false;
		}

		if (name.equals(mth.name) && descriptor.equals(mth.descriptor))
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	public int hashCode()
	{
		return hashCode;
	}
}






