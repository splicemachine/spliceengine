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

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

import com.splicemachine.db.iapi.sql.Activation;

import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.TransactionController;

import com.splicemachine.db.iapi.types.RowLocation;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;

/**
  Perform Index maintenace associated with DML operations for a table's
  indexes.
  */
public class IndexSetChanger
{
	//
	//Index row generators.
	IndexRowGenerator[] irgs;
	//
	//Index conglomerate ids. indexCIDS[ix] is the conglomerate id
	//for the index with IndexRowGenerator irgs[ix].
	long[] indexCIDS;
	private DynamicCompiledOpenConglomInfo[] indexDCOCIs;
	private StaticCompiledOpenConglomInfo[] indexSCOCIs;
	String[] indexNames;
	ConglomerateController baseCC;
	FormatableBitSet		baseRowReadMap;

	// TransactionController for management of temporary conglomerates
	TransactionController tc;

	TemporaryRowHolderImpl rowHolder;

	IndexChanger[] indexChangers;

	// Lock mode for the indexes
	private int lockMode;

	//Set on open.
	boolean[] fixOnUpdate;
	
	boolean isOpen = false;
	
	//
	//Name for the set of no indexes
	private static final int NO_INDEXES 		= 0;
	//
	//Name for the set of indexes we change on a update operation
	private static final int UPDATE_INDEXES  	= 1;
	//
	//Name for the set of all indexes.
	private static final int ALL_INDEXES		= 2;
	
	//
	//To start, no indexes are open.
	private int whatIsOpen = NO_INDEXES;

	private int isolationLevel;
	private final Activation activation;

	/**
	  Create a new IndexSetChanger

	  @param irgs the IndexRowGenerators for the table's indexes. We use
	    positions in this array as local id's for indexes.
	  @param indexCIDS the conglomerate ids for the table's indexes.
	  	indexCIDS[ix] corresponds to the same index as irgs[ix].
	  @param indexSCOCIs the SCOCIs for the table's idexes. 
	  	indexSCOCIs[ix] corresponds to the same index as irgs[ix].
	  @param indexDCOCIs the DCOCIs for the table's idexes. 
	  	indexDCOCIs[ix] corresponds to the same index as irgs[ix].
	  @param baseCC a ConglomerateController for the base table.
	  @param tc	a TransactionController for managing temporary conglomerates
	  @param lockMode	The lock mode (granularity) for the indexes.
	  @param baseRowReadMap Map of columns read in.  1 based.
	  @param isolationLevel	Isolation level to use
	  @param activation	Current activation
	  @exception StandardException		Thrown on error
	  */
	public IndexSetChanger(IndexRowGenerator[] irgs,
						   long[] indexCIDS,
						   StaticCompiledOpenConglomInfo[] indexSCOCIs,
						   DynamicCompiledOpenConglomInfo[] indexDCOCIs,
						   String[] indexNames,
						   ConglomerateController baseCC,
						   TransactionController tc,
						   int lockMode,
						   FormatableBitSet baseRowReadMap,
						   int isolationLevel,
						   Activation activation)
		 throws StandardException
	{
		this.irgs = irgs;
		this.indexCIDS = indexCIDS;
		this.indexSCOCIs = indexSCOCIs;
		this.indexDCOCIs = indexDCOCIs;
		this.indexNames = indexNames;
		this.baseCC = baseCC;
		this.tc = tc;
		this.lockMode = lockMode;
		this.baseRowReadMap = baseRowReadMap;
		this.isolationLevel = isolationLevel;
		this.activation = activation;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexCIDS != null, "indexCIDS is null");
		}

		indexChangers = new IndexChanger[irgs.length];
	}
	
	/**
	  Open this IndexSetchanger.

	  @param fixOnUpdate indicates which indexes to correct due
	    to an update. The entries in this array must be in the
		same order as the entries in the irgs array that was
		passed to the constructor.

	  @exception StandardException		Thrown on error
	  */
	public void open(boolean[] fixOnUpdate)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
		    SanityManager.ASSERT( ! isOpen, "IndexSetChanger already open");

		this.fixOnUpdate = fixOnUpdate;
		isOpen = true;
	}

	/**
	 * Set the row holder for all underlying changers to use.
	 * If the row holder is set, underlying changers  wont bother 
	 * saving copies of rows needed for deferred
	 * processing.  Also, it will never close the
	 * passed in rowHolder.
	 *
	 * @param rowHolder	the row holder
	 */
	public void setRowHolder(TemporaryRowHolderImpl rowHolder)
	{
		this.rowHolder = rowHolder;
	}

	/**
	  Open the indexes that must be fixed if they are not already
	  open.

	  @param whatToOpen must be one of ALL_INDEXES or UPDATE_INDEXES.
	  @exception StandardException		Thrown on error
	  */
	private void openIndexes(int whatToOpen)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
		    SanityManager.ASSERT( isOpen, "IndexSetChanger closed");

		if (whatIsOpen >= whatToOpen) return;
			
		for (int ix = 0; ix < indexChangers.length; ix++)
		{
			if (whatToOpen == UPDATE_INDEXES &&
				!fixOnUpdate[ix])
				continue;
			
			/* Instantiate an index changer, if it doesn't exist,
			 * otherwise we propagate the CC for the heap to
			 * the index changer.
			 */
			if (indexChangers[ix] == null)
			{
				/* DataDictionary doesn't have compiled info. */
				indexChangers[ix] =
					new IndexChanger(irgs[ix],
									 indexCIDS[ix],
									 (indexSCOCIs == null) ? 
										 (StaticCompiledOpenConglomInfo) null :
											indexSCOCIs[ix],
									 (indexDCOCIs == null) ? 
										 (DynamicCompiledOpenConglomInfo) null :
											indexDCOCIs[ix],
									 (indexNames == null) ? null : 
									                        indexNames[ix],
									 baseCC,
									 tc,
									 lockMode,
									 baseRowReadMap,
									 isolationLevel,
									 activation);
				indexChangers[ix].setRowHolder(rowHolder);
			}
			else
			{
				indexChangers[ix].setBaseCC(baseCC);
			}
			indexChangers[ix].open();
		}
		whatIsOpen = whatToOpen;
	}

	/**
	  Perform index maintenance associated with deleting a row
	  from a table.

	  @param baseRow the deleted row.
	  @param baseRowLocation the deleted row's base conglomerate
	     location
	  @exception StandardException		Thrown on error
	  */
	public void delete(ExecRow baseRow,
					   RowLocation baseRowLocation)
		 throws StandardException
	{
		openIndexes(ALL_INDEXES);
        for (IndexChanger indexChanger : indexChangers) indexChanger.delete(baseRow, baseRowLocation);
	}

	/**
	  Perform index maintenance associated with insering a row
	  into a table.

	  @param baseRow the row.
	  @param baseRowLocation the row's base conglomerate
	     location
	  @exception StandardException		Thrown on error
	  */
	public void insert(ExecRow baseRow,
					   RowLocation baseRowLocation)
		 throws StandardException
	{
		openIndexes(ALL_INDEXES);
        for (IndexChanger indexChanger : indexChangers) indexChanger.insert(baseRow, baseRowLocation);
	}

	/**
	  Perform index maintenance associated with updating a row
	  in a table.

	  @param oldBaseRow the old image of the row.
	  @param newBaseRow the new image of the row.
	  @param baseRowLocation the row's base conglomerate
	     location
	  @exception StandardException		Thrown on error
	  */
	public void update(ExecRow oldBaseRow,
					   ExecRow newBaseRow,
					   RowLocation baseRowLocation)
		 throws StandardException
	{
		openIndexes(UPDATE_INDEXES);
		for (int ix = 0; ix < indexChangers.length; ix++)
			if (fixOnUpdate[ix])
				indexChangers[ix].update(oldBaseRow,
										 newBaseRow,
										 baseRowLocation);
	}

	/**
	 * Propagate the heap's ConglomerateController to
	 * all of the underlying index changers.
	 *
	 * @param baseCC	The heap's ConglomerateController.
	 */
	public void setBaseCC(ConglomerateController baseCC)
	{
        for (IndexChanger indexChanger : indexChangers) {
            if (indexChanger != null) {
                indexChanger.setBaseCC(baseCC);
            }
        }
		this.baseCC = baseCC;
	}

	/**
	  Finish processing the changes for this IndexSetChanger.  This means
	  doing the deferred inserts for updates of unique indexes.

	  @exception StandardException		Thrown on error
	 */
	public void finish()
		throws StandardException
	{
        for (IndexChanger indexChanger : indexChangers) {
            if (indexChanger != null) {
                indexChanger.finish();
            }
        }
	}
		
	/**
	  Close this IndexSetChanger.

	  @exception StandardException		Thrown on error
	  */
	public void close()
		throws StandardException
	{
		whatIsOpen = NO_INDEXES;
        for (IndexChanger indexChanger : indexChangers) {
            if (indexChanger != null) {
                indexChanger.close();
            }
        }
		fixOnUpdate = null;
		isOpen = false;
		rowHolder = null;
	}

	/**
	  Create a string describing the state of this IndexSetChanger
	  */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String whatIsOpen_s = null;
			switch (whatIsOpen)
			{
			case NO_INDEXES:
				whatIsOpen_s = "No open indexes ";
				break;
			case UPDATE_INDEXES:
				whatIsOpen_s = "Update indexes open ";
				break;
			case ALL_INDEXES:
				whatIsOpen_s = "All indexes open ";
				break;
			default:
				SanityManager.THROWASSERT("bad whatIsOpen value "+whatIsOpen);
				break;
			}

			StringBuilder fixOnUpdate_s = new StringBuilder("fixOnUpdate=(");
			for (int ix = 0; ix < fixOnUpdate.length; ix++)
			{
				if (ix > 0)
					fixOnUpdate_s.append(",");

                fixOnUpdate_s.append(fixOnUpdate[ix]);
			}
			fixOnUpdate_s.append(")");

			StringBuilder indexDesc_s = new StringBuilder("\n");
			for (int ix = 0; ix < indexCIDS.length; ix++)
			{
				if (indexChangers[ix] == null)
					indexDesc_s.append("    Index[").append(ix).append("] cid=").append(indexCIDS[ix]).append(" closed. \n");
                else
					indexDesc_s.append("    ").append(indexChangers[ix].toString()).append("\n");
			}

			return "IndexSetChanger: "+
				whatIsOpen_s+
				fixOnUpdate_s+
				indexDesc_s;
		}

		return null;
	}
}
