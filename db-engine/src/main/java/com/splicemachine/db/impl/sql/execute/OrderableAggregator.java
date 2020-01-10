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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * Abstract aggregator for Orderable aggregates (max/min).
 *
 */
abstract class OrderableAggregator extends SystemAggregator
{
	protected DataValueDescriptor value;

	/**
	 */
	public ExecAggregator setup( ClassFactory cf, String aggregateName, DataTypeDescriptor returnDataType )
	{
			return this;
	}

	/**
	 * @see ExecAggregator#merge
	 *
	 * @exception StandardException on error
	 */
	public void merge(ExecAggregator addend)
		throws StandardException
	{
			/*
			 * Derby(pre-splice) requires that both sides of the aggregate field be initialized. This is
			 * an expensive proposition, because it requires object creation for every aggregate for every row, which
			 * rapidly adds up.
			 *
			 * However, if we avoid initialization on every row (instead doing it once for each DISTINCT row), then
			 * we must make sure that our individual aggregators properly handle null addends. For OrderableAggregators,
			 * we can treat null as "not contributing". That is, for sums, null is equivalent to adding 0; for min
			 * and max computations, null is ignored as not contributory. In either case, we can just return directly.
			 * This involves a null check for every row, but that is substantially cheaper than an object creation.
			 */
			if(addend==null) return;
			if (SanityManager.DEBUG)
			{
					SanityManager.ASSERT(addend instanceof OrderableAggregator,
									"addend is supposed to be the same type of aggregator for the merge operator");
			}

			// Don't bother merging if the other has never been used.
			DataValueDescriptor bv = ((OrderableAggregator)addend).value;
			if (bv != null)
					this.accumulate(bv);
	}

	public void add(DataValueDescriptor addend) throws StandardException{
		this.accumulate(addend);
	}	

	/**
	 * Return the result of the operations that we
	 * have been performing.  Returns a DataValueDescriptor.
	 *
	 * @return the result as a DataValueDescriptor 
	 */
	public DataValueDescriptor getResult() throws StandardException
	{
		return value;
	}
        public String toString()
        {
            return "OrderableAggregator: " + value;
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
	 * ourselves back in.  We rely on formatable to handle
	 * situations where <I>value</I> is null.
	 * <p>
	 * Why would we be called to write ourselves out if we
	 * are null?  For scalar aggregates, we don't bother
	 * setting up the aggregator since we only need a single
	 * row.  So for a scalar aggregate that needs to go to
	 * disk, the aggregator might be null.
	 * 
	 * @exception IOException on error
	 *
	 * @see java.io.Externalizable#writeExternal
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		super.writeExternal(out);
		out.writeObject(value);
	}

	/** 
	 * @see java.io.Externalizable#readExternal 
	 *
	 * @exception IOException on error
	 * @exception ClassNotFoundException on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		value = (DataValueDescriptor) in.readObject();
	}
}
