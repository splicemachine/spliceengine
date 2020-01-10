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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.types.*;

/**
 * Aggregator for SUM().  Defers most of its work
 * to OrderableAggregator.
 *
 */
public  class SumAggregator 
	extends OrderableAggregator
{
		@Override
		public ExecAggregator setup(ClassFactory cf, String aggregateName, DataTypeDescriptor returnDataType) {
				ExecAggregator bufferedAggregator = getBufferedAggregator(returnDataType);
				if(bufferedAggregator==null) bufferedAggregator = this;
				return bufferedAggregator;
		}

		public static SumAggregator getBufferedAggregator(DataTypeDescriptor returnDataType) {
				int typeFormatId = returnDataType.getTypeId().getTypeFormatId();
				/*
				 * Note that we always use longs to aggregate any scalar type, so tinyint, smallint, integer,
				 * and longint all will use a longBufferedAggregator.
				 *
				 * This is mostly just for clarity. The Query Optimizer makes everything return a long anyway, so
				 * nothing ever pops through with an integer (as of v0.6 anyway).
				 */
				switch(typeFormatId){
						case StoredFormatIds.TINYINT_TYPE_ID:
						case StoredFormatIds.SMALLINT_TYPE_ID:
						case StoredFormatIds.INT_TYPE_ID:
						case StoredFormatIds.LONGINT_TYPE_ID:
								return new LongBufferedSumAggregator(64); //todo -sf- make this configurable?
						case StoredFormatIds.REAL_TYPE_ID:
						case StoredFormatIds.DOUBLE_TYPE_ID:
								return new DoubleBufferedSumAggregator(64);
						case StoredFormatIds.DECIMAL_TYPE_ID:
						    return new DecimalBufferedSumAggregator(64);
						default: //default to Derby's typical behavior, which has crappy performance, but will work in all cases
								return null;
				}
		}

		/**
	 * Accumulate
 	 *
	 * @param addend	value to be added in
	 *
	 * @exception StandardException on error
	 *
	 * @see ExecAggregator#accumulate
	 */
	protected void accumulate(DataValueDescriptor addend) 
		throws StandardException
	{

		/*
		** If we don't have any value yet, just clone
		** the addend.
		*/
		if (value == null)
		{ 
			/* NOTE: We need to call cloneValue since value gets
			 * reused underneath us
			 */
			if (addend instanceof SQLInteger ||
			    addend instanceof SQLTinyint ||
			    addend instanceof SQLSmallint)
			{
				SQLLongint l = new SQLLongint();
				l.setValue(addend.getLong());
				value = l;
			}
			else
			{
				value = addend.cloneValue(false);
			}
		}
		else
		{
			NumberDataValue	input = (NumberDataValue)addend;
			NumberDataValue nv = (NumberDataValue) value;

				value = nv.plus(
								input,						// addend 1
								nv,		// addend 2
								nv);	// result
		}
	}

		public void add(DataValueDescriptor addend) throws StandardException{
				accumulate(addend);
		}

		/**
		 * @return ExecAggregator the new aggregator
		 */
		public ExecAggregator newAggregator()
		{
				return new SumAggregator();
		}

		////////////////////////////////////////////////////////////
		//
		// FORMATABLE INTERFACE
		//
		/////////////////////////////////////////////////////////////
		/**
		 * Get the formatID which corresponds to this class.
		 *
		 *	@return	the formatID of this class
		 */
		public	int	getTypeFormatId()	{ return StoredFormatIds.AGG_SUM_V01_ID; }

		public String toString()
		{
				try {
						return "SumAggregator: " + (value!= null ? value.getString() : "NULL");
				}
				catch (StandardException e)
				{
						return super.toString() + ":" + e.getMessage();
				}
		}
}
