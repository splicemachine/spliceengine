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

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.DependencyDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SystemColumn;
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.UserType;

/**
 * Factory for creating a SYSDEPENDSS row.
 *
 */

public class SYSDEPENDSRowFactory extends CatalogRowFactory
{
	private static final String		TABLENAME_STRING = "SYSDEPENDS";

	protected static final int		SYSDEPENDS_COLUMN_COUNT = 4;
	protected static final int		SYSDEPENDS_DEPENDENTID = 1;
	protected static final int		SYSDEPENDS_DEPENDENTTYPE = 2;
	protected static final int		SYSDEPENDS_PROVIDERID = 3;
	protected static final int		SYSDEPENDS_PROVIDERTYPE = 4;

	protected static final int		SYSDEPENDS_INDEX1_ID = 0;
	protected static final int		SYSDEPENDS_INDEX2_ID = 1;

	
    private	static	final	boolean[]	uniqueness = {
		                                               false,
													   false
	                                                 };

	private static final int[][] indexColumnPositions =
	{
		{SYSDEPENDS_DEPENDENTID},
		{SYSDEPENDS_PROVIDERID}
	};

	private	static	final	String[]	uuids =
	{
		 "8000003e-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000043-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000040-00d0-fd77-3ed8-000a0a0b1900"	// SYSDEPENDS_INDEX1
		,"80000042-00d0-fd77-3ed8-000a0a0b1900"	// SYSDEPENDS_INDEX2
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public SYSDEPENDSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo(SYSDEPENDS_COLUMN_COUNT,TABLENAME_STRING, indexColumnPositions,
				 uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSDEPENDS row
	 *
	 * @param td DependencyDescriptor. If its null then we want to make an empty
	 * row. 
	 *
	 * @return	Row suitable for inserting into SYSDEPENDS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor	td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		ExecRow    				row;
		String					dependentID = null;
		DependableFinder		dependentBloodhound = null;
		String					providerID = null;
		DependableFinder		providerBloodhound = null;

		if (td != null)
		{
			DependencyDescriptor dd = (DependencyDescriptor)td;
			dependentID	= dd.getUUID().toString();
			dependentBloodhound = dd.getDependentFinder();
			if ( dependentBloodhound == null )
			{
				throw StandardException.newException(SQLState.DEP_UNABLE_TO_STORE);
			}

			providerID	= dd.getProviderID().toString();
			providerBloodhound = dd.getProviderFinder();
			if ( providerBloodhound == null )
			{
				throw StandardException.newException(SQLState.DEP_UNABLE_TO_STORE);
			}

		}

		/* Insert info into sysdepends */

		/* RESOLVE - It would be nice to require less knowledge about sysdepends
		 * and have this be more table driven.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSDEPENDS_COLUMN_COUNT);

		/* 1st column is DEPENDENTID (UUID - char(36)) */
		row.setColumn(SYSDEPENDS_DEPENDENTID, new SQLChar(dependentID));

		/* 2nd column is DEPENDENTFINDER */
		row.setColumn(SYSDEPENDS_DEPENDENTTYPE,
				new UserType(dependentBloodhound));

		/* 3rd column is PROVIDERID (UUID - char(36)) */
		row.setColumn(SYSDEPENDS_PROVIDERID, new SQLChar(providerID));

		/* 4th column is PROVIDERFINDER */
		row.setColumn(SYSDEPENDS_PROVIDERTYPE,
				new UserType(providerBloodhound));

		return row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a ConstraintDescriptor out of a SYSDEPENDS row
	 *
	 * @param row a SYSDEPENDSS row
	 * @param parentTupleDescriptor	Null for this kind of descriptor.
	 * @param dd dataDictionary
	 *
	 * @exception   StandardException thrown on failure
	 */
	public TupleDescriptor buildDescriptor(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd )
					throws StandardException
	{
		DependencyDescriptor dependencyDesc = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSDEPENDS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSDEPENDS row");
		}

		DataValueDescriptor	col;
		String				dependentIDstring;
		UUID				dependentUUID;
		DependableFinder	dependentBloodhound;
		String				providerIDstring;
		UUID				providerUUID;
		DependableFinder	providerBloodhound;

		/* 1st column is DEPENDENTID (UUID - char(36)) */
		col = row.getColumn(SYSDEPENDS_DEPENDENTID);
		dependentIDstring = col.getString();
		dependentUUID = getUUIDFactory().recreateUUID(dependentIDstring);

		/* 2nd column is DEPENDENTTYPE */
		col = row.getColumn(SYSDEPENDS_DEPENDENTTYPE);
		dependentBloodhound = (DependableFinder) col.getObject();

		/* 3rd column is PROVIDERID (UUID - char(36)) */
		col = row.getColumn(SYSDEPENDS_PROVIDERID);
		providerIDstring = col.getString();
		providerUUID = getUUIDFactory().recreateUUID(providerIDstring);

		/* 4th column is PROVIDERTYPE */
		col = row.getColumn(SYSDEPENDS_PROVIDERTYPE);
		providerBloodhound = (DependableFinder) col.getObject();

		/* now build and return the descriptor */
		return new DependencyDescriptor(dependentUUID, dependentBloodhound,
										   providerUUID, providerBloodhound);
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[]	buildColumnList()
        throws StandardException
	{
            return new SystemColumn[] {
                SystemColumnImpl.getUUIDColumn("DEPENDENTID", false),
                SystemColumnImpl.getJavaColumn("DEPENDENTFINDER",
                        "com.splicemachine.db.catalog.DependableFinder", false),
                SystemColumnImpl.getUUIDColumn("PROVIDERID", false),
                SystemColumnImpl.getJavaColumn("PROVIDERFINDER",
                        "com.splicemachine.db.catalog.DependableFinder", false),
           };
	}
}
