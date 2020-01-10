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

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 *	Class for implementation of DependableFinder in the core DataDictionary 
 *	for referenced columns in a table.
 *
 *
 */

public class DDColumnDependableFinder extends DDdependableFinder
{
	private static final long serialVersionUID = 1L;
	////////////////////////////////////////////////////////////////////////
	//
	//  STATE
	//
	////////////////////////////////////////////////////////////////////////

	// write least amount of data to disk, just the byte array, not even
	// a FormatableBitSet
	private byte[] columnBitMap;

    ////////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTORS
    //
    ////////////////////////////////////////////////////////////////////////

	/**
	 * Serialization Constructor. DO NOT USE
	 */
	public DDColumnDependableFinder()
	{
		
	}

	/**
	 * Constructor same as in parent.
	 */
	public  DDColumnDependableFinder(int formatId)
	{
		super(formatId);
	}

	/**
	 * Constructor given referenced column bit map byte array as in FormatableBitSet
	 */
	public  DDColumnDependableFinder(int formatId, byte[] columnBitMap)
	{
		super(formatId);
		this.columnBitMap = columnBitMap;
	}

    ////////////////////////////////////////////////////////////////////////
    //
    //  DDColumnDependable METHODS
    //
    ////////////////////////////////////////////////////////////////////////

	/**
	 * Get the byte array encoding the bitmap of referenced columns in
	 * a table.
	 *
	 * @return		byte array as in a FormatableBitSet encoding column bit map
	 */
	public 	byte[]	getColumnBitMap()
	{
		return columnBitMap;
	}

	/**
	 * Set the byte array encoding the bitmap of referenced columns in
	 * a table.
	 *
	 * @param	columnBitMap	byte array as in a FormatableBitSet encoding column bit map
	 */
	public	void	setColumnBitMap(byte[] columnBitMap)
	{
		this.columnBitMap = columnBitMap;
	}

	/**
	 * Find a dependable object, which is essentially a table descriptor with
	 * referencedColumnMap field set.
	 *
	 * @param	dd data dictionary
	 * @param	dependableObjectID dependable object ID (table UUID)
	 * @return	a dependable, a table descriptor with referencedColumnMap
	 *			field set
	 */
	Dependable findDependable(DataDictionary dd, UUID dependableObjectID)
		throws StandardException
	{
		TableDescriptor td = dd.getTableDescriptor(dependableObjectID);
		if (td != null)  // see beetle 4444
			td.setReferencedColumnMap(new FormatableBitSet(columnBitMap));
		return td;
	}

    //////////////////////////////////////////////////////////////////
    //
    //  FORMATABLE METHODS
    //
    //////////////////////////////////////////////////////////////////

	/**
	 * Read this object from a stream of stored objects.  Just read the
	 * byte array, besides what the parent does.
	 *
	 * @param in read this.
	 */
	public void readExternal( ObjectInput in )
			throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		FormatableHashtable fh = (FormatableHashtable)in.readObject();
		columnBitMap = (byte[])fh.get("columnBitMap");
	}

	/**
	 * Write this object to a stream of stored objects.  Just write the
	 * byte array, besides what the parent does.
	 *
	 * @param out write bytes here.
	 */
	public void writeExternal( ObjectOutput out )
			throws IOException
	{
		super.writeExternal(out);
		FormatableHashtable fh = new FormatableHashtable();
		fh.put("columnBitMap", columnBitMap);
		out.writeObject(fh);
	}
}
