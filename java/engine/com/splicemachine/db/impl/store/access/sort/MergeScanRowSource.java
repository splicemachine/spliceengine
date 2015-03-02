/*

   Derby - Class com.splicemachine.db.impl.store.access.sort.MergeScanRowSource

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

package com.splicemachine.db.impl.store.access.sort;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.ScanControllerRowSource;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.access.SortObserver;

import com.splicemachine.db.iapi.types.DataValueDescriptor;

import com.splicemachine.db.iapi.types.RowLocation;

import java.util.Vector;

// For JavaDoc references (i.e. @see)
import com.splicemachine.db.iapi.store.access.RowLocationRetRowSource;

/**
	Wrapping the output of a MergeScan in a RowSource for the benefit of the
	createAndLoadConglomerate and loadConglomerate interface.  The output of a

	MergeScan is written to a file when we need more than one level of merge
	runs. 

	MergeScan implements ScanController, this class just implements the
	RowSource interface. 
*/
public class MergeScanRowSource extends MergeScan implements ScanControllerRowSource
{

    /* Constructors for This class: */
	MergeScanRowSource(
    MergeSort           sort, 
    TransactionManager  tran,
    SortBuffer          sortBuffer, 
    Vector              mergeRuns,
	SortObserver		sortObserver,
    boolean             hold)
    {
		super(sort, tran, sortBuffer, mergeRuns, sortObserver, hold);
    }

	/*
	 * Disable illegal and dangerous scan controller interface call
	 * @exception StandardException This is an illegal operation
	 */
	public boolean next() throws StandardException
	{
		throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
	}

    /* Private/Protected methods of This class: */
    /* Public Methods of This class: */
    /* Public Methods of RowSource class: */


    public DataValueDescriptor[] getNextRowFromRowSource() 
        throws StandardException
    {
		DataValueDescriptor[] row = sortBuffer.removeFirst();

		if (row != null)
		{
			mergeARow(sortBuffer.getLastAux());
		}

		return row;
	}

	/**
	 * @see RowLocationRetRowSource#needsRowLocation
	 */
	public boolean needsRowLocation()
	{
		return false;
	}

	/**
	 * @see com.splicemachine.db.iapi.store.access.RowSource#needsToClone
	 */
	public boolean needsToClone()
	{
		return false;
	}


	/**
	 * @see RowLocationRetRowSource#rowLocation
	 */
	public void rowLocation(RowLocation rl)
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("unexpected call to RowSource.rowLocation");
	}


	/**
		All columns are always set from a sorter
	*/
	public FormatableBitSet getValidColumns()
	{
		return null;
	}

	/**
		Close the row source - implemented by MergeScan already
	 */
	public void closeRowSource()
	{
		close();
	}

}

