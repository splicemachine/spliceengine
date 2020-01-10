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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.iapi.types.TypeId;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static com.splicemachine.db.iapi.types.NumberDataValue.MAX_DECIMAL_PRECISION_SCALE;

/**
 Aggregator for AVG(). Extends the SumAggregator and
 implements a count. Result is then sum()/count().
 To handle overflow we catch the exception for
 value out of range, then we swap the holder for
 the current sum to one that can handle a larger
 range. Eventually a sum may end up in a SQLDecimal
 which can handle an infinite range. Once this
 type promotion has happened, it will not revert back
 to the original type, even if the sum would fit in
 a lesser type.

 */
public final class AvgAggregator extends OrderableAggregator
{
		private SumAggregator aggregator;
		private long count;
		private int scale;

		public Class getAggregatorClass() { return aggregator.getClass();}

		public boolean usesLongBufferedSumAggregator() {
		    return aggregator != null && aggregator instanceof LongBufferedSumAggregator;
		}
		public void upgradeSumAggregator() throws StandardException {
		    if (usesLongBufferedSumAggregator()) {
		        LongBufferedSumAggregator lbsa = (LongBufferedSumAggregator) aggregator;
		        aggregator = lbsa.upgrade();
            }
        }

		@Override
		public ExecAggregator setup(ClassFactory cf, String aggregateName, DataTypeDescriptor returnDataType) {
				this.aggregator = SumAggregator.getBufferedAggregator(returnDataType);
				if(aggregator==null)
						aggregator = new SumAggregator();
				switch(returnDataType.getTypeId().getTypeFormatId()){
						case StoredFormatIds.TINYINT_TYPE_ID:
						case StoredFormatIds.SMALLINT_TYPE_ID:
						case StoredFormatIds.INT_TYPE_ID:
						case StoredFormatIds.LONGINT_TYPE_ID:
								scale = 0;
								break;
						case StoredFormatIds.REAL_TYPE_ID:
						case StoredFormatIds.SQL_DOUBLE_ID:
								scale = TypeId.DECIMAL_SCALE;
								break;
						default:
							// Honor the scale of the return data type picked by the
							// parser.  In the past, this was overridden, which may
							// cause overflows.  For averaging DEC(38,0), 38 digits
							// of precision is enough.  We don't want to tell the user
							// we can't calculate the average because we require 42
							// digits of precision in this case.
							scale = returnDataType.getScale();
				}
				return this;
		}

		protected void accumulate(DataValueDescriptor addend)
						throws StandardException
		{

//				if (count == 0) {
//						String typeName = addend.getTypeName();
//						if (   typeName.equals(TypeId.TINYINT_NAME)
//										|| typeName.equals(TypeId.SMALLINT_NAME)
//										|| typeName.equals(TypeId.INTEGER_NAME)
//										|| typeName.equals(TypeId.LONGINT_NAME)) {
//								scale = 0;
//						} else if (   typeName.equals(TypeId.REAL_NAME)
//										|| typeName.equals(TypeId.DOUBLE_NAME)) {
//								scale = TypeId.DECIMAL_SCALE;
//						} else {
//								// DECIMAL
//								scale = ((NumberDataValue) addend).getDecimalValueScale();
//								if (scale < NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE)
//										scale = NumberDataValue.MIN_DECIMAL_DIVIDE_SCALE;
//						}
//				}

				try {

				    	count++;
						aggregator.accumulate(addend);
						return;

				} catch (StandardException se) {

					    // DOUBLE has the largest range, so if it overflows, there is nothing
					    // with greater range to upgrade to.
						if (!se.getMessageId().equals(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE) ||
                            (!(aggregator instanceof LongBufferedSumAggregator)))
							throw se;
				}


		/*
			Sum is out of range so promote

			TINYINT,SMALLINT -->> INTEGER

			INTEGER -->> BIGINT

			REAL -->> DOUBLE PRECISION

			others -->> DECIMAL
		*/
				/*
				 * -sf- Note: When summing scalar values, we always return a long (as of v0.6 anyway). This means
				 * that TINYINT,SMALLINT,INTEGER,and BIGINT are all already sharing a type, which means if we overflowed,
				 * we overflowed a long. As a result, we have the following upgrade schedule:
				 *
				 * SCALAR(long) -> DOUBLE
				 * REAL					->	DOUBLE
				 * DOUBLE				->	DECIMAL
				 *
				 * Note also that we don't need to re-accumulate the passed in addend if we have a BufferedSumAggregator,
				 * because the value of addend must be present in the buffer before an overflow failure can occur. This
				 * means that we can just call "upgrade" to move the aggregator to the correct type, but otherwise we don't
				 * have to do anything
				 */
				if(aggregator instanceof LongBufferedSumAggregator){
					aggregator = ((LongBufferedSumAggregator)aggregator).upgrade();
				}
				else
				    throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE);

				aggregator.accumulate(addend);
		}

		public void merge(ExecAggregator addend)
						throws StandardException
		{
				if(addend==null) return; //treat null entries as zero --they do not contribute to the average
				AvgAggregator otherAvg = (AvgAggregator) addend;
				this.aggregator.merge(otherAvg.aggregator);
				this.count+= otherAvg.count;

				// if I haven't been used take the other.
//				if (count == 0) {
//						count = otherAvg.count;
//						value = otherAvg.value;
//						scale = otherAvg.scale;
//						return;
//				}

				// Don't bother merging if the other is a NULL value aggregate.
		/* Note:Beetle:5346 fix change the sort to be High, that makes
		 * the neccessary for the NULL check because after the change 
		 * addend could have a  NULL value even on distincts unlike when 
		 * NULLs were sort order  Low, because by  sorting NULLs Low  
		 * they  happens to be always first row which makes it as 
		 * aggreagte result object instead of addends.
		 * Query that will fail without the following check:
		 * select avg(a) , count(distinct a) from t1;
		*/
//				if(otherAvg.value != null)
//				{
//						// subtract one here as the accumulate will add one back in
//						count += (otherAvg.count - 1);
//						accumulate(otherAvg.value);
//				}
		}

		public void add(DataValueDescriptor addend) throws StandardException{
				accumulate(addend);
		}

		/**
		 * Return the result of the aggregation.  If the count
		 * is zero, then we haven't averaged anything yet, so
		 * we return null.  Otherwise, return the running
		 * average as a double.
		 *
		 * @return null or the average as Double
		 */
		public DataValueDescriptor getResult() throws StandardException
		{
				if (count == 0)
				{
						return null;
				}

				NumberDataValue sum = (NumberDataValue)aggregator.getResult();
				NumberDataValue avg = (NumberDataValue) sum.getNewNull();


				if (count > (long) Integer.MAX_VALUE)
				{
						// TINYINT, SMALLINT, INTEGER implement arithmetic using integers
						// If the sum is still represented as a TINYINT, SMALLINT or INTEGER
						// we cannot let their int based arithmetic handle it, since they
						// will perform a getInt() on the long value which will truncate the long value.
						// One solution would be to promote the sum to a SQLLongint, but its value
						// will be less than or equal to Integer.MAX_VALUE, so the average will be 0.
						String typeName = sum.getTypeName();

						if (typeName.equals(TypeId.INTEGER_NAME) ||
										typeName.equals(TypeId.TINYINT_NAME) ||
										typeName.equals(TypeId.SMALLINT_NAME))
						{
								avg.setValue(0);
								return avg;
						}
				}

				NumberDataValue countv = new com.splicemachine.db.iapi.types.SQLLongint(count);
				sum.divide(sum, countv, avg, scale);

				return avg;
		}

        @Override
		public ExecAggregator newAggregator()
		{
				AvgAggregator avgAggregator = new AvgAggregator();
				avgAggregator.aggregator = (SumAggregator)this.aggregator.newAggregator();
                avgAggregator.scale = this.scale;
                return avgAggregator;
		}

		/////////////////////////////////////////////////////////////
		//
		// EXTERNALIZABLE INTERFACE
		//
		/////////////////////////////////////////////////////////////
		/**
		 *
		 * @exception IOException on error
		 */
		public void writeExternal(ObjectOutput out) throws IOException
		{
				out.writeObject(aggregator);
				out.writeBoolean(eliminatedNulls);
				out.writeLong(count);
				out.writeInt(scale);
		}

		/**
		 * @see java.io.Externalizable#readExternal
		 *
		 * @exception IOException on error
		 */
		public void readExternal(ObjectInput in)
						throws IOException, ClassNotFoundException
		{
				aggregator = (SumAggregator)in.readObject(); //TODO -sf- is this the most efficient? perhaps better to get the sum direct
				eliminatedNulls = in.readBoolean();
				count = in.readLong();
				scale = in.readInt();
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
		public	int	getTypeFormatId()	{ return StoredFormatIds.AGG_AVG_V01_ID; }
		public String toString()
		{
				try {
						return "AvgAggregator: { sum=" + (value!= null?value.getString():"NULL") + ", count=" + count + "}";
				}
				catch (StandardException e)
				{
						return super.toString() + ":" + e.getMessage();
				}
		}
}
