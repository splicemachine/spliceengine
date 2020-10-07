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

package com.splicemachine.db.impl.sql;

import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.iapi.util.StringUtil;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * GenericResultDescription: basic implementation of result
 * description, used in conjunction with the other 
 * implementations in this package.  This implementation 
 * of ResultDescription may be used by anyone.
 *
 */
public final class GenericResultDescription
	implements ResultDescription, Formatable
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

	private ResultColumnDescriptor[] columns;
	private String statementType;
    
    /**
     * Saved JDBC ResultSetMetaData object.
     * @see ResultDescription#setMetaData(java.sql.ResultSetMetaData)
     */
    private transient ResultSetMetaData metaData;
    
    /**
     * A map which maps a column name to a column number.
     * Entries only added when accessing columns with the name.
     */
    private Map columnNameMap;
	
	/**
	 * Niladic constructor for Formatable
	 */
	public GenericResultDescription()
	{
	}

	/**
	 * Build a GenericResultDescription from columns and type
	 *
	 * @param columns an array of col descriptors
	 * @param statementType the type
	 */
	public GenericResultDescription(ResultColumnDescriptor[] columns, 
					String statementType) 
	{
		this.columns = columns;
		this.statementType = statementType;
	}

	/**
	 * Build a GenericResultDescription 
	 *
	 * @param rd the result description
	 * @param theCols the columns to take from the input rd
	 */
	public GenericResultDescription
	(
		ResultDescription	rd, 
		int[]				theCols
	) 
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(theCols != null, "theCols argument to GenericResultDescription is null");
		}

		this.columns = new ResultColumnDescriptor[theCols.length];
		for (int i = 0; i < theCols.length; i++)
		{
			columns[i] = rd.getColumnDescriptor(theCols[i]);
		}
		this.statementType = rd.getStatementType();
	}

	//
	// ResultDescription interface
	//
	/**
	 * @see ResultDescription#getStatementType
	 */
	public String	getStatementType() {
		return statementType;
	}

	/**
	 * @see ResultDescription#getColumnCount
	 */
	public int	getColumnCount() 
	{
		return (columns == null) ? 0 : columns.length;
	}

	public ResultColumnDescriptor[] getColumnInfo() {
		return columns;
	}

	/**
	 * position is 1-based.
	 * @see ResultDescription#getColumnDescriptor
	 */
	public ResultColumnDescriptor getColumnDescriptor(int position) {
		return columns[position-1];
	}

	/**
	 * Get a new result description that has been truncated
	 * from input column number.   If the input column is
	 * 5, then columns 5 to getColumnCount() are removed.
	 * The new ResultDescription points to the same
	 * ColumnDescriptors (this method performs a shallow
	 * copy.
	 *
	 * @param truncateFrom the starting column to remove
	 *
	 * @return a new ResultDescription
	 */
	public ResultDescription truncateColumns(int truncateFrom)	
	{
		if (SanityManager.DEBUG) 
		{
			if (!(truncateFrom > 0 && columns != null))
			{
				SanityManager.THROWASSERT("bad truncate value: "+truncateFrom+" is too low");
			}
			if (truncateFrom > columns.length)
			{
				SanityManager.THROWASSERT("bad truncate value: "+truncateFrom+" is too high");
			}
		}
		ResultColumnDescriptor[] newColumns = new ResultColumnDescriptor[truncateFrom-1];
		System.arraycopy(columns, 0, newColumns, 0, newColumns.length);
		return new GenericResultDescription(newColumns, statementType);
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
		int len = (columns == null) ? 0 : columns.length;

		out.writeObject(statementType);
		out.writeInt(len);
		while(len-- > 0)
		{
			/*
			** If we don't have a GenericColumnsDescriptor, 
			** create one now and use that to write out.
			** Do this to avoid writing out query tree
			** implementations of ResultColumnDescriptor
			*/
			if (!(columns[len] instanceof 
						GenericColumnDescriptor))
			{
				columns[len] = new GenericColumnDescriptor(columns[len]);
			}
			out.writeObject(columns[len]);
		}
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
		int len;

		columns = null;
		statementType = (String)in.readObject();
		len = in.readInt();
		if (len > 0)
		{
			columns = new GenericColumnDescriptor[len];
			while(len-- > 0)
			{
				columns[len] = (ResultColumnDescriptor)in.readObject();
			}
		}
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int getTypeFormatId()	{ return StoredFormatIds.GENERIC_RESULT_DESCRIPTION_V01_ID; }


	
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			StringBuilder colStr = new StringBuilder();
			for (int i = 0; i < columns.length; i++)
			{
				colStr.append("column[").append(i).append("]\n");
				colStr.append(columns[i].toString());
			}	
			return "GenericResultDescription\n" +
					"\tStatementType = "+statementType+"\n" +
					"\tCOLUMNS\n" + colStr.toString();
		}
		else
		{
			return "";
		}
	}

    /**
     * Set the meta data if it has not already been set.
     */
    public synchronized void setMetaData(ResultSetMetaData rsmd) {
        if (metaData == null)
            metaData = rsmd;
    }

    /**
     * Get the saved meta data.
     */
    public synchronized ResultSetMetaData getMetaData() {
        return metaData;
    }

    /**
     * Find a column name based upon the JDBC rules for
     * getXXX and setXXX. Name matching is case-insensitive,
     * matching the first name (1-based) if there are multiple
     * columns that map to the same name.
     */
    public int findColumnInsenstive(String columnName) {
        
        final Map workMap; 
        
        synchronized (this) {
            if (columnNameMap==null) {
                // updateXXX and getXXX methods are case insensitive and the 
                // first column should be returned. The loop goes backward to 
                // create a map which preserves this property.
                Map map = new HashMap();
                for (int i = getColumnCount(); i>=1; i--) {
                    
                    final String key = StringUtil.
                        SQLToUpperCase(
                            getColumnDescriptor(i).getName());
                    
                    final Integer value = ReuseFactory.getInteger(i);
                    
                    map.put(key, value);
                }
                
                // Ensure this map can never change.
                columnNameMap = Collections.unmodifiableMap(map);
            }
            workMap = columnNameMap;
        }
        
        Integer val = (Integer) workMap.get(columnName);
        if (val==null) {
            val = (Integer) workMap.get(StringUtil.SQLToUpperCase(columnName));
        }
        if (val==null) {
            return -1;
        } else {
            return val;
        }
    }
}

