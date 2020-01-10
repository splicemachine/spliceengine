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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.store.access.ColumnOrdering;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.Formatable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
/**
	Basic implementation of ColumnOrdering.
	Not sure what to tell callers about 0-based versus 1-based numbering.
	Assume 0-based for now.

 */
public class IndexColumnOrder implements ColumnOrdering, Formatable
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

	int colNum;
	boolean ascending;
        /**
         * indicate whether NULL values should sort low.
         *
         * nullsOrderedLow is usually false, because generally Derby defaults
         * to have NULL values compare higher than non-null values, but if
         * the user specifies an ORDER BY clause with a <null ordering>
         * specification that indicates that NULL values should be ordered
         * lower than non-NULL values, thien nullsOrderedLow is set to true.
         */
        boolean nullsOrderedLow;

	/*
	 * class interface
	 */

	/**
	 * Niladic constructor for formatable
	 */
	public IndexColumnOrder() 
	{
	}

	public IndexColumnOrder(int colNum) {
		 this.colNum = colNum;
		 this.ascending = true;
                 this.nullsOrderedLow = false;
	}

	public IndexColumnOrder(int colNum, boolean ascending) {
		 this.colNum = colNum;
		 this.ascending = ascending;
                 this.nullsOrderedLow = false;
	}

        /**
         * constructor used by the ORDER BY clause.
         *
         * This version of the constructor is used by the compiler when
         * it processes an ORDER BY clause in a SQL statement. For such
         * statements, the user gets to control whether NULL values are
         * ordered as lower than all non-NULL values, or higher than all
         * non-NULL values.
         *
         * @param colNum number of this column
         * @param ascending whether the ORDER BY is ascendeing or descending
         * @param nullsLow whether nulls should be ordered low
         */
	public IndexColumnOrder(int colNum, boolean ascending,
                boolean nullsLow)
        {
		 this.colNum = colNum;
		 this.ascending = ascending;
                 this.nullsOrderedLow = nullsLow;
	}

	/*
	 * ColumnOrdering interface
 	 */
	public int getColumnId() {
		return colNum;
	}

	public boolean getIsAscending() {
		return ascending;
	}

        /**
         * Indicate whether NULL values should be ordered below non-NULL.
         *
         * This function returns TRUE if the user has specified, via the
         * <null ordering> clause in the ORDER BY clause, that NULL values
         * of this column should sort lower than non-NULL values.
         *
         * @return whether nulls should sort low
         */
	public boolean getIsNullsOrderedLow() {
		return nullsOrderedLow;
	}

	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write this object out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeInt(colNum);
		out.writeBoolean(ascending);
		out.writeBoolean(nullsOrderedLow);
	}

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException, ClassNotFoundException
	{
		colNum = in.readInt();
		ascending = in.readBoolean();
		nullsOrderedLow = in.readBoolean();
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.INDEX_COLUMN_ORDER_V01_ID; }

	public String toString() {
		if (SanityManager.DEBUG) {
			return
				"IndexColumnOrder.colNum: " + colNum + "\n" +
				"IndexColumnOrder.ascending: " + ascending  + "\n" +
				"IndexColumnOrder.nullsOrderedLow: " + nullsOrderedLow;
		} else {
			return super.toString();
		}

	}
}
