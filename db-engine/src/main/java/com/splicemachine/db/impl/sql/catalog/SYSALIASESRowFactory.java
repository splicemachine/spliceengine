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

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;


/**
 * Factory for creating a SYSALIASES row.
 *
 * Here are the directions for adding a new system supplied alias.
 * Misc:
 *  All system supplied aliases are class aliases at this point.
 *	Additional arrays will need to be added if we supply system
 *	aliases of other types.
 *	The preloadAliasIDs array is an array of hard coded UUIDs
 *	for the system supplied aliases.
 *	The preloadAliases array is the array of aliases
 *	for the system supplied aliases.  This array is in alphabetical
 *	order by package and class in Xena.  Each alias is the uppercase
 *	class name of the alias.
 *  The preloadJavaClassNames array is the array of full package.class
 *  names for the system supplied aliases.  This array is in alphabetical
 *	order by package and class in Xena.  
 *	SYSALIASES_NUM_BOOT_ROWS is the number of boot rows in sys.sysaliases
 *  in a new database.
 *
 *
 */

public class SYSALIASESRowFactory extends CatalogRowFactory
{

	private static final int		SYSALIASES_COLUMN_COUNT = 9;
	private static final int		SYSALIASES_ALIASID = 1;
	private static final int		SYSALIASES_ALIAS = 2;
	private static final int		SYSALIASES_SCHEMAID = 3;
	private static final int		SYSALIASES_JAVACLASSNAME = 4;
	private static final int		SYSALIASES_ALIASTYPE = 5;
	private static final int		SYSALIASES_NAMESPACE = 6;
	private static final int		SYSALIASES_SYSTEMALIAS = 7;
	public  static final int		SYSALIASES_ALIASINFO = 8;
	private static final int		SYSALIASES_SPECIFIC_NAME = 9;

 
	protected static final int		SYSALIASES_INDEX1_ID = 0;

	protected static final int		SYSALIASES_INDEX2_ID = 1;

	protected static final int		SYSALIASES_INDEX3_ID = 2;

	// null means all unique.
    private	static	final	boolean[]	uniqueness = null;

	private static int[][] indexColumnPositions =
	{
		{SYSALIASES_SCHEMAID, SYSALIASES_ALIAS, SYSALIASES_NAMESPACE},
		{SYSALIASES_ALIASID},
		{SYSALIASES_SCHEMAID, SYSALIASES_SPECIFIC_NAME},
	};

	private	static	final	String[]	uuids =
	{
		 "c013800d-00d7-ddbd-08ce-000a0a411400"	// catalog UUID
		,"c013800d-00d7-ddbd-75d4-000a0a411400"	// heap UUID
		,"c013800d-00d7-ddbe-b99d-000a0a411400"	// SYSALIASES_INDEX1
		,"c013800d-00d7-ddbe-c4e1-000a0a411400"	// SYSALIASES_INDEX2
		,"c013800d-00d7-ddbe-34ae-000a0a411400"	// SYSALIASES_INDEX3
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public SYSALIASESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf, DataDictionary dd)
	{
		super(uuidf,ef,dvf,dd);
		initInfo(SYSALIASES_COLUMN_COUNT, "SYSALIASES", indexColumnPositions, uniqueness, uuids);
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSALIASES row
	 *
	 *
	 * @return	Row suitable for inserting into SYSALIASES.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(boolean latestVersion, TupleDescriptor	td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		String					schemaID = null;
		String					javaClassName = null;
		String					sAliasType = null;
		String					aliasID = null;
		String					aliasName = null;
		String					specificName = null;
		char					cAliasType = AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR;
		char					cNameSpace = AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR;
		boolean					systemAlias = false;
		AliasInfo				aliasInfo = null;

		if (td != null && (td instanceof AliasDescriptor)) {
			
			AliasDescriptor 		ad = (AliasDescriptor)td;
			aliasID	= ad.getUUID().toString();
			aliasName = ad.getDescriptorName();
			schemaID	= ad.getSchemaUUID().toString();
			javaClassName	= ad.getJavaClassName();
			cAliasType = ad.getAliasType();
			cNameSpace = ad.getNameSpace();
			systemAlias = ad.getSystemAlias();
			aliasInfo = ad.getAliasInfo();
			specificName = ad.getSpecificName();

			char[] charArray = new char[1];
			charArray[0] = cAliasType;
			sAliasType = new String(charArray);

			if (SanityManager.DEBUG)
			{
				switch (cAliasType)
				{
					case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
					case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
					case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
					case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
					case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
						break;

					default:
						SanityManager.THROWASSERT(
							"Unexpected value (" + cAliasType +
							") for aliasType");
				}
			}
		}


		/* Insert info into sysaliases */

		/* RESOLVE - It would be nice to require less knowledge about sysaliases
		 * and have this be more table driven.
		 */

		/* Build the row to insert */
		ExecRow row = getExecutionFactory().getValueRow(SYSALIASES_COLUMN_COUNT);

		/* 1st column is ALIASID (UUID - char(36)) */
		row.setColumn(SYSALIASES_ALIASID, new SQLChar(aliasID));

		/* 2nd column is ALIAS (varchar(128))) */
		row.setColumn(SYSALIASES_ALIAS, new SQLVarchar(aliasName));
		//		System.out.println(" added row-- " + aliasName);

		/* 3rd column is SCHEMAID (UUID - char(36)) */
		row.setColumn(SYSALIASES_SCHEMAID, new SQLChar(schemaID));

		/* 4th column is JAVACLASSNAME (longvarchar) */
		row.setColumn(SYSALIASES_JAVACLASSNAME, dvf.getLongvarcharDataValue(javaClassName));

		/* 5th column is ALIASTYPE (char(1)) */
		row.setColumn(SYSALIASES_ALIASTYPE, new SQLChar(sAliasType));

		/* 6th column is NAMESPACE (char(1)) */
		String sNameSpace = new String(new char[] { cNameSpace });

		row.setColumn
			(SYSALIASES_NAMESPACE, new SQLChar(sNameSpace));


		/* 7th column is SYSTEMALIAS (boolean) */
		row.setColumn
			(SYSALIASES_SYSTEMALIAS, new SQLBoolean(systemAlias));

		/* 8th column is ALIASINFO (com.splicemachine.db.catalog.AliasInfo) */
		row.setColumn(SYSALIASES_ALIASINFO, 
			new UserType(aliasInfo));

		/* 9th column is specific name */
		row.setColumn
			(SYSALIASES_SPECIFIC_NAME, new SQLVarchar(specificName));


		return row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a AliasDescriptor out of a SYSALIASES row
	 *
	 * @param row a SYSALIASES row
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
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSALIASES_COLUMN_COUNT, 
				"Wrong number of columns for a SYSALIASES row");
		}

		char				cAliasType;
		char				cNameSpace;
		DataValueDescriptor	col;
		String				aliasID;
		UUID				aliasUUID;
		String				aliasName;
		String				javaClassName;
		String				sAliasType;
		String				sNameSpace;
		String				typeStr;
		boolean				systemAlias = false;
		AliasInfo			aliasInfo = null;

		/* 1st column is ALIASID (UUID - char(36)) */
		col = row.getColumn(SYSALIASES_ALIASID);
		aliasID = col.getString();
		aliasUUID = getUUIDFactory().recreateUUID(aliasID);

		/* 2nd column is ALIAS (varchar(128)) */
		col = row.getColumn(SYSALIASES_ALIAS);
		aliasName = col.getString();

		/* 3rd column is SCHEMAID (UUID - char(36)) */
		col = row.getColumn(SYSALIASES_SCHEMAID);
		UUID schemaUUID = col.isNull() ? null : getUUIDFactory().recreateUUID(col.getString());

		/* 4th column is JAVACLASSNAME (longvarchar) */
		col = row.getColumn(SYSALIASES_JAVACLASSNAME);
		javaClassName = col.getString();

		/* 5th column is ALIASTYPE (char(1)) */
		col = row.getColumn(SYSALIASES_ALIASTYPE);
		sAliasType = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(sAliasType.length() == 1, 
				"Fifth column (aliastype) type incorrect");
			switch (sAliasType.charAt(0))
			{
				case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR: 
				case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR: 
				case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR: 
				case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR: 
				case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
					break;

				default: 
					SanityManager.THROWASSERT("Invalid type value '"
							+sAliasType+ "' for  alias");
			}
		}

		cAliasType = sAliasType.charAt(0);

		/* 6th column is NAMESPACE (char(1)) */
		col = row.getColumn(SYSALIASES_NAMESPACE);
		sNameSpace = col.getString();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(sNameSpace.length() == 1, 
				"Sixth column (namespace) type incorrect");
			switch (sNameSpace.charAt(0))
			{
				case AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR: 
				case AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR: 
				case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR: 
				case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR: 
				case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
					break;

				default: 
					SanityManager.THROWASSERT("Invalid type value '"
							+sNameSpace+ "' for  alias");
			}
		}

		cNameSpace = sNameSpace.charAt(0);


		/* 7th column is SYSTEMALIAS (boolean) */
		col = row.getColumn(SYSALIASES_SYSTEMALIAS);
		systemAlias = col.getBoolean();

		/* 8th column is ALIASINFO (com.splicemachine.db.catalog.AliasInfo) */
		col = row.getColumn(SYSALIASES_ALIASINFO);
		aliasInfo = (AliasInfo) col.getObject();

		/* 9th column is specific name */
		col = row.getColumn(SYSALIASES_SPECIFIC_NAME);
		String specificName = col.getString();


		/* now build and return the descriptor */
		return new AliasDescriptor(dd, aliasUUID, aliasName,
										schemaUUID, javaClassName, cAliasType,
										cNameSpace, systemAlias,
										aliasInfo, specificName);
	}

    /**
     * Builds a list of columns suitable for creating this Catalog.
     * DERBY-1734 fixed an issue where older code created the
     * BOOLEAN column SYSTEMALIAS with maximum length 0 instead of 1.
     * DERBY-1742 was opened to track if upgrade changes are needed.
     *
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[]   buildColumnList()
        throws StandardException
    {
      return new SystemColumn[] {
        
        SystemColumnImpl.getUUIDColumn("ALIASID", false),
        SystemColumnImpl.getIdentifierColumn("ALIAS", false),
        SystemColumnImpl.getUUIDColumn("SCHEMAID", true),
        SystemColumnImpl.getColumn("JAVACLASSNAME",
                java.sql.Types.LONGVARCHAR, false, Integer.MAX_VALUE),
        SystemColumnImpl.getIndicatorColumn("ALIASTYPE"),
        SystemColumnImpl.getIndicatorColumn("NAMESPACE"),
        SystemColumnImpl.getColumn("SYSTEMALIAS",
                Types.BOOLEAN, false),
        SystemColumnImpl.getJavaColumn("ALIASINFO",
                "com.splicemachine.db.catalog.AliasInfo", true),
        SystemColumnImpl.getIdentifierColumn("SPECIFICNAME", false)
        };
    }

    public List<ColumnDescriptor[]> getViewColumns(TableDescriptor view, UUID viewId) throws StandardException {

        List<ColumnDescriptor[]> cdsl = new ArrayList<>();
        cdsl.add(
                new ColumnDescriptor[]{
                        new ColumnDescriptor("SCHEMANAME",1,1,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("ALIAS",2,2,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("BASETABLE",3,3,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 256),
                                null,null,view,viewId,0,0,0)
                });
        return cdsl;
    }
    public static String SYSALIAS_TO_TABLE_VIEW_SQL = "create view SYSALIASTOTABLEVIEW as \n" +
            "SELECT S.SCHEMANAME, A.alias as ALIAS, cast(A.ALIASINFO as varchar(256)) as BASETABLE \n" +
            "FROM \n" +
            "SYS.SYSALIASES A, SYS.SYSTABLES T, SYSVW.SYSSCHEMASVIEW S \n" +
            "WHERE A.ALIASTYPE = 'S' AND \n" +
            "S.SCHEMAID = T.SCHEMAID AND \n" +
            "A.SCHEMAID = T.SCHEMAID AND \n" +
            "A.ALIAS = T.TABLENAME";

}
