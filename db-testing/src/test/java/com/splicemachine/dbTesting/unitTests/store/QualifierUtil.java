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

package com.splicemachine.dbTesting.unitTests.store;

import com.splicemachine.db.iapi.store.access.Qualifier;

import com.splicemachine.db.iapi.types.DataValueDescriptor;

class QualifierUtil implements Qualifier 
{
    private int                 column_id;
    private int                 storagePosition;
    private DataValueDescriptor key_val;
    private int                 operator;
    private boolean             negateCompareResult;
    private boolean             orderedNulls;
    private boolean             unknownRV;

    /**
     * Constuctor
     */
    public QualifierUtil(
    int                 column_id,
    DataValueDescriptor key_val,
    int                 operator,
    boolean             negateCompareResult,
    boolean             orderedNulls,
    boolean             unknownRV)
    {
        this.column_id              = column_id;
        this.storagePosition        = column_id;
        this.key_val                = key_val;
        this.operator               = operator;
        this.negateCompareResult    = negateCompareResult;
        this.orderedNulls           = orderedNulls;
        this.unknownRV              = unknownRV;
    }

    public QualifierUtil(
        int                 column_id,
        int                 storagePosition,
        DataValueDescriptor key_val,
        int                 operator,
        boolean             negateCompareResult,
        boolean             orderedNulls,
        boolean             unknownRV)
    {
        this.column_id              = column_id;
        this.storagePosition        = storagePosition;
        this.key_val                = key_val;
        this.operator               = operator;
        this.negateCompareResult    = negateCompareResult;
        this.orderedNulls           = orderedNulls;
        this.unknownRV              = unknownRV;
    }

    /** Qualifier interface: **/

    /** Get the id of the column to be qualified. **/
    public int getColumnId()
    {
        return(this.column_id);
    }

    public int getStoragePosition()
    {
        return(this.storagePosition);
    }

    /** Get the value that the column is to be compared to. **/
    public DataValueDescriptor getOrderable()
    {
        return(this.key_val);
    }

    /** Get the operator to use in the comparison. 
     *
     *  @see DataValueDescriptor#compare
     **/
    public int getOperator()
    {
        return(this.operator);
    }

    /** Should the result of the compare be negated?
     *
     *  @see DataValueDescriptor#compare
     **/
    public boolean negateCompareResult()
    {
        return(this.negateCompareResult);
    }

    /** Get the getOrderedNulls argument to use in the comparison. 
     *  
     *  @see DataValueDescriptor#compare
     **/
    public boolean getOrderedNulls()
    {
        return(this.orderedNulls);
    }

    /** Get the getOrderedNulls argument to use in the comparison.
     *  
     *  @see DataValueDescriptor#compare
     **/
    public boolean getUnknownRV()
    {
        return(this.unknownRV);
    }

	/** Clear the DataValueDescriptor cache, if one exists.
	 *  (The DataValueDescriptor can be 1 of 3 types:
	 *		o  VARIANT		  - cannot be cached as its value can 
	 *							vary within a scan
	 *		o  SCAN_INVARIANT - can be cached within a scan as its
	 *							value will not change within a scan
	 *		o  QUERY_INVARIANT- can be cached across the life of the query
	 *							as its value will never change
	 *		o  CONSTANT		  - can be cached across executions
     *  
     *  @see Qualifier#getUnknownRV
	 */
	public void clearOrderableCache()
	{
		// No Orderable caching here
	}

    @Override
    public int getVariantType() {
        return VARIANT;
    }

    /**
	 * This method reinitializes all the state of
	 * the Qualifier.  It is used to distinguish between
	 * resetting something that is query invariant
	 * and something that is constant over every
	 * execution of a query.  Basically, clearOrderableCache()
	 * will only clear out its cache if it is a VARIANT
	 * or SCAN_INVARIANT value.  However, each time a
	 * query is executed, the QUERY_INVARIANT qualifiers need
	 * to be reset.
	 */
	public void reinitialize()
	{
	}

    public String getText() {
        return null;
    }
}
