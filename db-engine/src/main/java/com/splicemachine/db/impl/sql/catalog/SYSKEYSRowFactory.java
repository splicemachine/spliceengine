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

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.KeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SubKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SystemColumn;
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLChar;

/**
 * Factory for creating a SYSKEYS row.
 *
 */

public class SYSKEYSRowFactory extends CatalogRowFactory
{
	private static final String		TABLENAME_STRING = "SYSKEYS";

	protected static final int		SYSKEYS_COLUMN_COUNT = 2;
	protected static final int		SYSKEYS_CONSTRAINTID = 1;
	protected static final int		SYSKEYS_CONGLOMERATEID = 2;

	public static final int		SYSKEYS_INDEX1_ID = 0;

    private	static	final	boolean[]	uniqueness = null;

	private static final int[][] indexColumnPositions =
	{
		{SYSKEYS_CONSTRAINTID}
	};

	private	static	final	String[]	uuids =
	{
		 "80000039-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"8000003c-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"8000003b-00d0-fd77-3ed8-000a0a0b1900"	// SYSKEYS_INDEX1
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public SYSKEYSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo(SYSKEYS_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSKEYS row
	 *
	 * @return	Row suitable for inserting into SYSKEYS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		ExecRow    				row;
		UUID						oid;
		String					constraintID = null;
		String					conglomerateID = null;

		if (td != null)
		{
			KeyConstraintDescriptor	constraint = (KeyConstraintDescriptor)td;

			/*
			** We only allocate a new UUID if the descriptor doesn't already have one.
			** For descriptors replicated from a Source system, we already have an UUID.
			*/
			oid = constraint.getUUID();
			constraintID = oid.toString();

			conglomerateID = constraint.getIndexUUIDString();
		}

		/* Insert info into syskeys */

		/* RESOLVE - It would be nice to require less knowledge about syskeys
		 * and have this be more table driven.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSKEYS_COLUMN_COUNT);

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		row.setColumn(SYSKEYS_CONSTRAINTID, new SQLChar(constraintID));
		/* 2nd column is CONGLOMERATEID (UUID - char(36)) */
		row.setColumn(SYSKEYS_CONGLOMERATEID, new SQLChar(conglomerateID));

		return row;
	}



	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a SubConstraintDescriptor out of a SYSKEYS row
	 *
	 * @param row a SYSKEYS row
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
		SubKeyConstraintDescriptor keyDesc = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSKEYS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSKEYS row");
		}

		DataValueDescriptor		col;
		DataDescriptorGenerator ddg;
		UUID					constraintUUID;
		UUID					conglomerateUUID;
		String				constraintUUIDString;
		String				conglomerateUUIDString;

		ddg = dd.getDataDescriptorGenerator();

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		col = row.getColumn(SYSKEYS_CONSTRAINTID);
		constraintUUIDString = col.getString();
		constraintUUID = getUUIDFactory().recreateUUID(constraintUUIDString);

		/* 2nd column is CONGLOMERATEID (UUID - char(36)) */
		col = row.getColumn(SYSKEYS_CONGLOMERATEID);
		conglomerateUUIDString = col.getString();
		conglomerateUUID = getUUIDFactory().recreateUUID(conglomerateUUIDString);

		/* now build and return the descriptor */

		keyDesc =  new SubKeyConstraintDescriptor(
										constraintUUID,
										conglomerateUUID);
		return keyDesc;
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
                SystemColumnImpl.getUUIDColumn("CONSTRAINTID", false),
                SystemColumnImpl.getUUIDColumn("CONGLOMERATEID", false),
            };
        }

}
