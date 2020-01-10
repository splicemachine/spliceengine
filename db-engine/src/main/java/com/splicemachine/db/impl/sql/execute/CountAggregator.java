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

import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * Aggregator for COUNT()/COUNT(*).  
 */
public final class CountAggregator 
	extends SystemAggregator
{
	private long value;
	private boolean isCountStar;

	/**
	 */
	public ExecAggregator setup( ClassFactory cf, String aggregateName, DataTypeDescriptor returnType )
	{
			isCountStar = aggregateName.equals("COUNT(*)");
			return this;
	}

	/**
	 * @see ExecAggregator#merge
	 *
	 * @exception	StandardException	on error
	 */
	public void merge(ExecAggregator addend)
		throws StandardException
	{
			if(addend==null)
					return; //assume null is the same thing as zero
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(addend instanceof CountAggregator,
				"addend is supposed to be the same type of aggregator for the merge operator");
		}

		value += ((CountAggregator)addend).value;
	}

	public void add(DataValueDescriptor addend) throws StandardException{
		value+=addend.getLong();
	}

	/**
	 * Return the result of the aggregation.  Just
	 * spit out the running count.
	 *
	 * @return the value as a Long 
	 */
	public DataValueDescriptor getResult()
	{
		return new com.splicemachine.db.iapi.types.SQLLongint(value);
	}


	/**
	 * Accumulate for count().  Toss out all nulls in this kind of count.
	 * Increment the count for count(*). Count even the null values.
	 *
	 * @param addend	value to be added in
	 * @param ga		the generic aggregator that is calling me
	 *
	 * @see ExecAggregator#accumulate
	 */
	public void accumulate(DataValueDescriptor addend, Object ga)
		throws StandardException
	{
		if (isCountStar)
			value++;
		else
			super.accumulate(addend, ga);
	}

	protected final void accumulate(DataValueDescriptor addend) {
			value++;
	}

	/**
	 * @return ExecAggregator the new aggregator
	 */
	public ExecAggregator newAggregator()
	{
		CountAggregator ca = new CountAggregator();
		ca.isCountStar = isCountStar;
		return ca;
	}

	public boolean isCountStar()
	{
		return isCountStar;
	}

	/////////////////////////////////////////////////////////////
	// 
	// EXTERNALIZABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/** 
	 * Although we are not expected to be persistent per se,
	 * we may be written out by the sorter temporarily.  So
	 * we need to be able to write ourselves out and read
	 * ourselves back in.  
	 * 
	 * @exception IOException thrown on error
	 */
	public final void writeExternal(ObjectOutput out) throws IOException
	{
		super.writeExternal(out);
		out.writeBoolean(isCountStar);
		out.writeLong(value);
	}

	/** 
	* @see java.io.Externalizable#readExternal 
	*
	* @exception IOException io exception
	* @exception ClassNotFoundException on error
	*/
	public final void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		isCountStar = in.readBoolean();
		value = in.readLong();
	}	
	/////////////////////////////////////////////////////////////
	// 
	// FORMATABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId() { return StoredFormatIds.AGG_COUNT_V01_ID; }
}
