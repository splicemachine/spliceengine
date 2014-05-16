/*

   Derby - Class org.apache.derby.impl.sql.execute.SumAggregator

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.*;

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
				int typeFormatId = returnDataType.getTypeId().getTypeFormatId();
				switch(typeFormatId){
						case StoredFormatIds.LONGINT_TYPE_ID:
								return new LongBufferedSumAggregator(64); //todo -sf- make this configurable?
						case StoredFormatIds.DOUBLE_TYPE_ID:
								return new DoubleBufferedSumAggregator(64);
						default: //default to Derby's typical behavior
								return super.setup(cf, aggregateName, returnDataType);
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
