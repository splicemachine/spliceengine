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

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SystemColumn;
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ViewDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * Factory for creating a SYSVIEWS row.
 *
 */

public class SYSVIEWSRowFactory extends CatalogRowFactory
{
	private  static final String	TABLENAME_STRING = "SYSVIEWS";

	protected static final int		SYSVIEWS_COLUMN_COUNT = 4;
	protected static final int		SYSVIEWS_TABLEID = 1;
	protected static final int		SYSVIEWS_VIEWDEFINITION = 2;
	protected static final int		SYSVIEWS_CHECKOPTION = 3;
	protected static final int		SYSVIEWS_COMPILATION_SCHEMAID = 4;

	// Column widths
	protected static final int		SYSVIEWS_TABLEID_WIDTH = 36;

	protected static final int		SYSVIEWS_INDEX1_ID = 0;

	private static final int[][] indexColumnPositions =
	{
		{SYSVIEWS_TABLEID}
	};


	// if you add a non-unique index allocate this array.
    private	static	final	boolean[]	uniqueness = null;

	private	static	final	String[]	uuids =
	{
		 "8000004d-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000050-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"8000004f-00d0-fd77-3ed8-000a0a0b1900"	// SYSVIEWS_INDEX1
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public SYSVIEWSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo(SYSVIEWS_COLUMN_COUNT, TABLENAME_STRING, 
				 indexColumnPositions, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSVIEWS row
	 *
	 * @return	Row suitable for inserting into SYSVIEWS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
		throws StandardException 
	{
		DataValueDescriptor		col;
		ExecRow    				row;
		String					tableID = null;
		String					compSchemaId = null;
		String					viewText = null;
		String	   				checkSType = null;
		int	   					checkIType;

		if (td != null)
		{
			UUID	tableUUID;
			ViewDescriptor vd = (ViewDescriptor)td;

			/*
			** We only allocate a new UUID if the descriptor doesn't already have one.
			** For descriptors replicated from a Source system, we already have an UUID.
			*/
			tableUUID = vd.getUUID();
			if ( tableUUID == null )
		    {
				tableUUID = getUUIDFactory().createUUID();
				vd.setUUID(tableUUID);
			}
			tableID = tableUUID.toString();
			viewText = vd.getViewText();

			/* RESOLVE - check constraints not supported yet */
			checkIType = vd.getCheckOptionType();

			if (SanityManager.DEBUG)
			{
				if (checkIType != ViewDescriptor.NO_CHECK_OPTION)
				{
					SanityManager.THROWASSERT("checkIType expected to be " + 
						ViewDescriptor.NO_CHECK_OPTION +
						", not " + checkIType);
				}
			}
			checkSType = "N";

			UUID tmpId = vd.getCompSchemaId();
			compSchemaId = (tmpId == null) ? null : tmpId.toString();
		}

		/* Insert info into sysviews */

		/* RESOLVE - It would be nice to require less knowledge about sysviews
		 * and have this be more table driven.
		 */

		/* Build the row to insert  */
		row = getExecutionFactory().getValueRow(SYSVIEWS_COLUMN_COUNT);

		/* 1st column is TABLEID (UUID - char(36)) */
		row.setColumn(SYSVIEWS_TABLEID, new SQLChar(tableID));

		/* 2nd column is VIEWDEFINITION */
		row.setColumn(SYSVIEWS_VIEWDEFINITION,
				dvf.getLongvarcharDataValue(viewText));

		/* 3rd column is CHECKOPTION (char(1)) */
		row.setColumn(SYSVIEWS_CHECKOPTION, new SQLChar(checkSType));

		/* 4th column is COMPILATIONSCHEMAID (UUID - char(36)) */
		row.setColumn(SYSVIEWS_COMPILATION_SCHEMAID, new SQLChar(compSchemaId));

		return row;
	}
    
	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a ViewDescriptor out of a SYSVIEWS row
	 *
	 * @param row a SYSVIEWS row
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
		ViewDescriptor vd = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSVIEWS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSVIEWS row");
		}

		DataValueDescriptor	col;
		DataDescriptorGenerator ddg;
		int					checkIType;
		String				checkSType;
		String				tableID;
		String				compSchemaId;
		String				viewDefinition;
		UUID				tableUUID;
		UUID				compSchemaUUID = null;

		ddg = dd.getDataDescriptorGenerator();

		/* 1st column is TABLEID (UUID - char(36)) */
		col = row.getColumn(SYSVIEWS_TABLEID);
		tableID = col.getString();
		tableUUID = getUUIDFactory().recreateUUID(tableID);

		/* 2nd column is VIEWDEFINITION */
		col = row.getColumn(SYSVIEWS_VIEWDEFINITION);
		viewDefinition = col.getString();

		/* 3rd column is CHECKOPTION (char(1)) */
		col = row.getColumn(SYSVIEWS_CHECKOPTION);
		checkSType = col.getString();

		if (SanityManager.DEBUG)
		{
			if (!checkSType.equals("N"))
			{
				SanityManager.THROWASSERT("checkSType expected to be 'N', not " + checkSType);
			}
		}

		/* RESOLVE - no check options for now */
		checkIType = ViewDescriptor.NO_CHECK_OPTION;

		/* 4th column is COMPILATIONSCHEMAID (UUID - char(36)) */
		col = row.getColumn(SYSVIEWS_COMPILATION_SCHEMAID);
		compSchemaId = col.getString();
		if (compSchemaId != null)
		{
			compSchemaUUID = getUUIDFactory().recreateUUID(compSchemaId);
		}

		/* now build and return the descriptor */
		vd = ddg.newViewDescriptor(tableUUID, null, viewDefinition, 
				checkIType, compSchemaUUID);
		return vd;
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
                SystemColumnImpl.getUUIDColumn("TABLEID", false),
                SystemColumnImpl.getColumn("VIEWDEFINITION", Types.LONGVARCHAR,
                        false, TypeId.LONGVARCHAR_MAXWIDTH),
                SystemColumnImpl.getIndicatorColumn("CHECKOPTION"),
                SystemColumnImpl.getUUIDColumn("COMPILATIONSCHEMAID", true),
                        
            };
	}
}
