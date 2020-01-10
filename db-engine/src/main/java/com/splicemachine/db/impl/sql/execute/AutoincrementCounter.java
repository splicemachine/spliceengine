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
import	com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.reference.SQLState;


/**
 * AutoincrementCounter is a not so general counter for the specific purposes
 * of autoincrement columns. It can be thought of as an in-memory autoincrement
 * column.
 * The counting or incrementing is done in fashion identical to the
 * AUTOINCREMENTVALUE in SYSCOLUMNS.
 * <p>
 * To create a counter, the user must call the constructor with a start value,
 * increment and optionally a final value. In addition the caller must specify
 * the schema name, table name and column name uniquely identifying the
 * counter. 
 * <p>
 * When a counter is created it is in an invalid state-- to initialize it, the
 * user must call either  <i>update</i> or <i>reset(false)</i>. The value of a
 * counter can be changed by either calling reset or update. 

 */
public class AutoincrementCounter 
{

	private Long start;
	private long increment;
	private String identity;
	private long finalValue;
	private String schemaName;
	private String tableName;
	private String columnName;
	// maintains state.
	private long counter;
	private int columnPosition;
	private boolean initialized = false;

	/**
	 * constructor 
	 * @param 	start		The start value of the counter; is a java object as
	 * 			it can also be null.
	 * @param   increment	how much to increment the counter by.
	 * @param	finalValue	the finalvalue of the counter. used by reset
	 * @param 	s
	 * @param   t
	 * @param	c
	 */
	public AutoincrementCounter(Long start, long increment, long finalValue,
								String s, String t, String c, int position)
	{
		this.increment = increment;
		this.start = start;
		this.initialized = false;
		this.identity = makeIdentity(s,t,c);
		this.finalValue = finalValue;
		this.schemaName = s;
		this.tableName = t;
		this.columnName = c;
		this.columnPosition = position;
		//		System.out.println("aic created with " + this);
	}

	/**
	 * make a unique key for the counter.
	 */
	public static String makeIdentity(String s, String t, String c)
	{
		return s + "." + t + "." + c;
	}

	/**
	 * make a unique key for the counter.
	 */
	public static String makeIdentity(TableDescriptor td, ColumnDescriptor cd)
	{
		return td.getSchemaName() + "." + td.getName() + 
				"." + cd.getColumnName();
	}

	/**
	 * reset to the counter to the beginning or the end.
	 * 
	 * @param 	begin	if TRUE reset to beginning and mark it uninitialized.
	 */
	public void reset(boolean begin)
	{
		if (begin)
			initialized = false;
		else
		{
			counter = finalValue;
			initialized = true;
		}
		//		System.out.println("counter reset to " + this);

	}

	/**
	 * update the counter.
	 * 
	 * @param 	t		update the counter to this value.
	 */
	public long update(long t)
	{
		counter = t;
		//		System.out.println("counter updated to " + this);
		initialized = true;
		return counter;
	}

	/**
	 * update the counter to its next value.
	 * 
	 * @exception	StandardException	if the counter has not yet been
	 * initialized and the Start value is NULL.
	 */
	public long update() throws StandardException
	{
		long counterVal;

		if (!initialized)
		{
			// The first time around, counter simply gets the start
			// value. 
			initialized = true;
			
			if (start == null)
			{
				throw StandardException.newException(
											SQLState.LANG_AI_COUNTER_ERROR);
			}
			counter = start;
		}	
		else
		{
			counter = counter + increment;
		}
		//		System.out.println("counter updated to " + this);
		return counter;
	}

	/**
	 * get the current value of the counter. An uninitialized counter means the
	 * current value is NULL.
	 */
	public Long getCurrentValue()
	{
		if (!initialized)
			return null;
		return counter;
	}
	
	/**
	 * return the identity of the counter.
	 */
	public String getIdentity()
	{
		return identity;
	}

	/**
	 * flush a counter to disk; i.e write the current value of the counter into
	 * the row in SYSCOLUMNS.
	 * 
	 * @param	tc			TransactionController to use
	 * @param	dd			DataDictionary to use.
	 * @param	tableUUID	I might have the table name but I need more
	 * information 
	 * @exception	StandardException standard Derby exception.
	 */
	public void flushToDisk(TransactionController tc, DataDictionary dd,
							UUID tableUUID)
	       throws StandardException
	{
		dd.setAutoincrementValue(tc, tableUUID, columnName, counter, true);
	}

	/**
	 * get the column position in the table for which this counter has been
	 * created. 
	 * @return the position of the corresponding column in the table (1-based)
	 */
	public int getColumnPosition()
	{
		return columnPosition;
	}

	/**
	 * get the start value
	 * @return the initial value of the counter
	 */
	public Long getStartValue()
	{
		return start;
	}

	public String toString()
	{
		return "counter: " + identity + " current: " + counter 
			+ " start: " + start + 
			" increment: " + increment + " final: " + finalValue;
	}	
}	


