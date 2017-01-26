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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.sql.execute.ScanQualifier;

import com.splicemachine.db.iapi.store.access.Qualifier;

import com.splicemachine.db.iapi.types.DataValueDescriptor;


/**
 *	This is the implementation for ScanQualifier.  It is used for system and user
 *  scans.
 *
 *	@version 0.1
 */

public class GenericScanQualifier implements ScanQualifier
{

	private int                 columnId        = -1;
    private int                 storagePosition = -1;
	private DataValueDescriptor orderable       = null;
	private int                 operator        = -1;
	private boolean             negateCR        = false;
	private boolean             orderedNulls    = false;
	private boolean             unknownRV       = false;

	private boolean             properInit      = false;

	public GenericScanQualifier() 
	{
	}

	/* 
	 * Qualifier interface
	 */

	/** 
	 * @see Qualifier#getColumnId
	 */
	public int getColumnId()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return columnId;
	}

    public int getStoragePosition()
    {
        return storagePosition;
    }

    /**
	 * @see Qualifier#getOrderable
	 */
	public DataValueDescriptor getOrderable()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return orderable;
	}

	/** Get the operator to use in the comparison. 
     *
     *  @see Qualifier#getOperator
     **/
	public int getOperator()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return operator;
	}

	/** Should the result from the compare operation be negated?  If true
     *  then only rows which fail the compare operation will qualify.
     *
     *  @see Qualifier#negateCompareResult
     **/
	public boolean negateCompareResult()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return negateCR;
	}

	/** Get the getOrderedNulls argument to use in the comparison. 
     *  
     *  @see Qualifier#getOrderedNulls
     **/
    public boolean getOrderedNulls()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return orderedNulls;
	}

	/** Get the getOrderedNulls argument to use in the comparison.
     *  
     *  @see Qualifier#getUnknownRV
     **/
    public boolean getUnknownRV()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return unknownRV;
	}

	/** Clear the DataValueDescriptor cache, if one exists.
	 *  (The DataValueDescriptor can be 1 of 3 types:
	 *		o  VARIANT		  - cannot be cached as its value can 
	 *							vary within a scan
	 *		o  SCAN_INVARIANT - can be cached within a scan as its
	 *							value will not change within a scan
	 *		o  QUERY_INVARIANT- can be cached across the life of the query
	 *							as its value will never change
	 *		o  CONSTANT		  - immutable
     *  
     *  @see Qualifier#getUnknownRV
	 */
	public void clearOrderableCache()
	{
		// No Orderable caching in ScanQualifiers
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

	/*
	 * ScanQualifier interface
	 */

	/**
	 * @see ScanQualifier#setQualifier
	 */
	public void setQualifier(
        int                 columnId,
        DataValueDescriptor orderable,
        int                 operator,
        boolean             negateCR,
        boolean             orderedNulls,
        boolean             unknownRV)
	{
		this.columnId = columnId;
        this.storagePosition = columnId;
		this.orderable = orderable;
		this.operator = operator;
		this.negateCR = negateCR;
		this.orderedNulls = orderedNulls;
		this.unknownRV = unknownRV;
		properInit = true;
	}

    public void setQualifier(
        int                 columnId,
        int                 storagePosition,
        DataValueDescriptor orderable,
        int                 operator,
        boolean             negateCR,
        boolean             orderedNulls,
        boolean             unknownRV)
    {
        this.columnId = columnId;
        this.storagePosition = storagePosition;
        this.orderable = orderable;
        this.operator = operator;
        this.negateCR = negateCR;
        this.orderedNulls = orderedNulls;
        this.unknownRV = unknownRV;
        properInit = true;
    }

    public String getText() {
        return null;
    }

	@Override
	public int getVariantType() {
		return SCAN_INVARIANT;
	}
}




