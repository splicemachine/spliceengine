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

import java.sql.Types;

import com.splicemachine.db.catalog.ReferencedColumns;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.CheckConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SubCheckConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SystemColumn;
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.UserType;

/**
 * Factory for creating a SYSCHECKS row.
 *
 */

public class SYSCHECKSRowFactory extends CatalogRowFactory
{
	private  static final String	TABLENAME_STRING = "SYSCHECKS";

	private static final int		SYSCHECKS_COLUMN_COUNT = 3;
	private static final int		SYSCHECKS_CONSTRAINTID = 1;
	private static final int		SYSCHECKS_CHECKDEFINITION = 2;
	private static final int		SYSCHECKS_REFERENCEDCOLUMNS = 3;

	static final int		SYSCHECKS_INDEX1_ID = 0;

	// index is unique.
    private	static	final	boolean[]	uniqueness = null;

	private static final int[][] indexColumnPositions =
	{	
		{SYSCHECKS_CONSTRAINTID}
	};

	private	static	final	String[]	uuids =
	{
		 "80000056-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000059-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000058-00d0-fd77-3ed8-000a0a0b1900"	// SYSCHECKS_INDEX1 UUID
	};



	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

	public SYSCHECKSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo(SYSCHECKS_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSCHECKS row
	 *
	 * @param td CheckConstraintDescriptorImpl
	 *
	 * @return	Row suitable for inserting into SYSCHECKS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor	td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		ExecIndexRow			row;
		ReferencedColumns rcd = null;
		String					checkDefinition = null;
		String					constraintID = null;

		if (td != null)
		{
			CheckConstraintDescriptor cd = (CheckConstraintDescriptor)td;
			/*
			** We only allocate a new UUID if the descriptor doesn't already have one.
			** For descriptors replicated from a Source system, we already have an UUID.
			*/
			constraintID = cd.getUUID().toString();

			checkDefinition = cd.getConstraintText();

			rcd = cd.getReferencedColumnsDescriptor();
		}

		/* Build the row */
		row = getExecutionFactory().getIndexableRow(SYSCHECKS_COLUMN_COUNT);

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		row.setColumn(SYSCHECKS_CONSTRAINTID, new SQLChar(constraintID));

		/* 2nd column is CHECKDEFINITION */
		row.setColumn(SYSCHECKS_CHECKDEFINITION,
				dvf.getLongvarcharDataValue(checkDefinition));

		/* 3rd column is REFERENCEDCOLUMNS
		 *  (user type com.splicemachine.db.catalog.ReferencedColumns)
		 */
		row.setColumn(SYSCHECKS_REFERENCEDCOLUMNS,
			new UserType(rcd));

		return row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a ViewDescriptor out of a SYSCHECKS row
	 *
	 * @param row a SYSCHECKS row
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
		SubCheckConstraintDescriptor checkDesc = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSCHECKS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSCHECKS row");
		}

		DataValueDescriptor		col;
		DataDescriptorGenerator ddg;
		ReferencedColumns	referencedColumns;
		String				constraintText;
		String				constraintUUIDString;
		UUID				constraintUUID;

		ddg = dd.getDataDescriptorGenerator();

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		col = row.getColumn(SYSCHECKS_CONSTRAINTID);
		constraintUUIDString = col.getString();
		constraintUUID = getUUIDFactory().recreateUUID(constraintUUIDString);

		/* 2nd column is CHECKDEFINITION */
		col = row.getColumn(SYSCHECKS_CHECKDEFINITION);
		constraintText = col.getString();

		/* 3rd column is REFERENCEDCOLUMNS */
		col = row.getColumn(SYSCHECKS_REFERENCEDCOLUMNS);
		referencedColumns =
			(ReferencedColumns) col.getObject();

		/* now build and return the descriptor */

		checkDesc = new SubCheckConstraintDescriptor(
										constraintUUID,
										constraintText,
										referencedColumns);
		return checkDesc;
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */

    public SystemColumn[] buildColumnList()
        throws StandardException
    {
        
       return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("CONSTRAINTID", false),
            SystemColumnImpl.getColumn("CHECKDEFINITION", Types.LONGVARCHAR, false),
            SystemColumnImpl.getJavaColumn("REFERENCEDCOLUMNS",
                    "com.splicemachine.db.catalog.ReferencedColumns", false)
        };
    }
}
