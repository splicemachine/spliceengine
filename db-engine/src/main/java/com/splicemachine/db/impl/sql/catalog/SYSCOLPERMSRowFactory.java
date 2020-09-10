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

import com.splicemachine.db.iapi.sql.dictionary.*;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for creating a SYSCOLPERMS row.
 *
 */

public class SYSCOLPERMSRowFactory extends PermissionsCatalogRowFactory
{
	static final String TABLENAME_STRING = "SYSCOLPERMS";

    // Column numbers for the SYSCOLPERMS table. 1 based
	private static final int COLPERMSID_COL_NUM = 1;
    private static final int GRANTEE_COL_NUM = 2;
    private static final int GRANTOR_COL_NUM = 3;
    private static final int TABLEID_COL_NUM = 4;
    private static final int TYPE_COL_NUM = 5;
    protected static final int COLUMNS_COL_NUM = 6;
    private static final int COLUMN_COUNT = 6;

    static final int GRANTEE_TABLE_TYPE_GRANTOR_INDEX_NUM = 0;
    static final int COLPERMSID_INDEX_NUM = 1;
    static final int TABLEID_INDEX_NUM = 2;
    protected static final int TOTAL_NUM_OF_INDEXES = 3;
	private static final int[][] indexColumnPositions = 
	{ 
		{ GRANTEE_COL_NUM, TABLEID_COL_NUM, TYPE_COL_NUM, GRANTOR_COL_NUM},
		{ COLPERMSID_COL_NUM },
		{ TABLEID_COL_NUM }
	};

    public static final int
        GRANTEE_COL_NUM_IN_GRANTEE_TABLE_TYPE_GRANTOR_INDEX = 1;

    private static final boolean[] indexUniqueness = { true, true, false};

    private	static final String[] uuids =
    {
        "286cc01e-0103-0e39-b8e7-00000010f010" // catalog UUID
		,"6074401f-0103-0e39-b8e7-00000010f010"	// heap UUID
		,"787c0020-0103-0e39-b8e7-00000010f010"	// index1
		,"c9a3808d-010c-42a2-ae15-0000000f67f8" //index2
		,"80220011-010c-bc85-060d-000000109ab8" //index3
    };

    public SYSCOLPERMSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf, DataDictionary dd)
	{
		super(uuidf,ef,dvf,dd);
		initInfo(COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, indexUniqueness, uuids);
	}

	public ExecRow makeRow(boolean latestVersion, TupleDescriptor td, TupleDescriptor parent) throws StandardException
	{
        UUID						oid;
        String colPermID = null;
        DataValueDescriptor grantee = null;
        DataValueDescriptor grantor = null;
        String tableID = null;
        String type = null;
        FormatableBitSet columns = null;

        if( td == null)
        {
            grantee = getNullAuthorizationID();
            grantor = getNullAuthorizationID();
        }
        else
        {
            if (!(td instanceof ColPermsDescriptor))
                throw new RuntimeException("Unexpected TupleDescriptor " + td.getClass().getName());

            ColPermsDescriptor cpd = (ColPermsDescriptor) td;
            oid = cpd.getUUID();
            if ( oid == null )
            {
            	oid = getUUIDFactory().createUUID();
            	cpd.setUUID(oid);           
            }
            colPermID = oid.toString();
            grantee = getAuthorizationID( cpd.getGrantee());
            grantor = getAuthorizationID( cpd.getGrantor());
            tableID = cpd.getTableUUID().toString();
            type = cpd.getType();
            columns = cpd.getColumns();
        }
        ExecRow row = getExecutionFactory().getValueRow( COLUMN_COUNT);
        row.setColumn( COLPERMSID_COL_NUM, new SQLChar(colPermID));
        row.setColumn( GRANTEE_COL_NUM, grantee);
        row.setColumn( GRANTOR_COL_NUM, grantor);
        row.setColumn( TABLEID_COL_NUM, new SQLChar(tableID));
        row.setColumn( TYPE_COL_NUM, new SQLChar(type));
        row.setColumn( COLUMNS_COL_NUM, new UserType( (Object) columns));
        return row;
    } // end of makeRow

	/** builds a tuple descriptor from a row */
	public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary	dataDictionary)
		throws StandardException
    {
        if( SanityManager.DEBUG)
            SanityManager.ASSERT( row.nColumns() == COLUMN_COUNT,
                                  "Wrong size row passed to SYSCOLPERMSRowFactory.buildDescriptor");

        String colPermsUUIDString = row.getColumn( COLPERMSID_COL_NUM).getString();
        UUID colPermsUUID = getUUIDFactory().recreateUUID(colPermsUUIDString);
        String tableUUIDString = row.getColumn( TABLEID_COL_NUM).getString();
        UUID tableUUID = getUUIDFactory().recreateUUID(tableUUIDString);
        String type = row.getColumn( TYPE_COL_NUM).getString();
        FormatableBitSet columns = (FormatableBitSet) row.getColumn( COLUMNS_COL_NUM).getObject();
        if( SanityManager.DEBUG)
            SanityManager.ASSERT( "s".equals( type) || "S".equals( type) ||
                                  "u".equals( type) || "U".equals( type) ||
                                  "r".equals( type) || "R".equals( type),
                                  "Invalid type passed to SYSCOLPERMSRowFactory.buildDescriptor");

        ColPermsDescriptor colPermsDesc =
	        new ColPermsDescriptor( dataDictionary, 
                    getAuthorizationID( row, GRANTEE_COL_NUM),
                    getAuthorizationID( row, GRANTOR_COL_NUM),
                    tableUUID, type, columns);
        colPermsDesc.setUUID(colPermsUUID);
        return colPermsDesc;
    } // end of buildDescriptor

	/** builds a column list for the catalog */
    public SystemColumn[] buildColumnList()
        throws StandardException
    {
        return new SystemColumn[] {
           SystemColumnImpl.getUUIDColumn("COLPERMSID", false),
           SystemColumnImpl.getIdentifierColumn("GRANTEE", false),
           SystemColumnImpl.getIdentifierColumn("GRANTOR", false),
           SystemColumnImpl.getUUIDColumn("TABLEID", false),
           SystemColumnImpl.getIndicatorColumn("TYPE"),
           SystemColumnImpl.getJavaColumn("COLUMNS",
                   "com.splicemachine.db.iapi.services.io.FormatableBitSet", false)
        };
    }

	/**
	 * builds an index key row for a given index number.
	 */
  	public ExecIndexRow buildIndexKeyRow( int indexNumber,
                                          PermissionsDescriptor perm) 
  		throws StandardException
    {
        ExecIndexRow row = null;
        
        switch( indexNumber)
        {
            case GRANTEE_TABLE_TYPE_GRANTOR_INDEX_NUM:
                // RESOLVE We do not support the FOR GRANT OPTION, so column permission rows are unique on the
                // grantee, table UUID, and type columns. The grantor column will always have the name of the owner of the
                // table. So the index key, used for searching the index, only has grantee, table UUID, and type columns.
                // It does not have a grantor column.
                //
                // If we support FOR GRANT OPTION then there may be multiple table permissions rows for a
                // (grantee, tableID, type) combination. We must either handle the multiple rows, which is necessary for
                // checking permissions, or add a grantor column to the key, which is necessary for granting or revoking
                // permissions.
                row = getExecutionFactory().getIndexableRow( 3);
                row.setColumn(1, getAuthorizationID( perm.getGrantee()));
                ColPermsDescriptor colPerms = (ColPermsDescriptor) perm;
                String tableUUIDStr = colPerms.getTableUUID().toString();
                row.setColumn(2, new SQLChar(tableUUIDStr));
                row.setColumn(3, new SQLChar(colPerms.getType()));
                break;
            case COLPERMSID_INDEX_NUM:
                row = getExecutionFactory().getIndexableRow( 1);
                String colPermsUUIDStr = perm.getObjectID().toString();
                row.setColumn(1, new SQLChar(colPermsUUIDStr));
                break;
            case TABLEID_INDEX_NUM:
                row = getExecutionFactory().getIndexableRow( 1);
                colPerms = (ColPermsDescriptor) perm;
                tableUUIDStr = colPerms.getTableUUID().toString();
                row.setColumn(1, new SQLChar(tableUUIDStr));
                break;
            default:
                break;

        }
        return row;
    } // end of buildIndexKeyRow
    
    public int getPrimaryKeyIndexNumber()
    {
        return GRANTEE_TABLE_TYPE_GRANTOR_INDEX_NUM;
    }

    /**
     * Or a set of permissions in with a row from this catalog table
     *
     * @param row an existing row
     * @param perm a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     * @param colsChanged An array with one element for each column in row. It is updated to
     *                    indicate which columns in row were changed
     *
     * @return The number of columns that were changed.
     *
     * @exception StandardException standard error policy
     */
    public int orPermissions( ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
        throws StandardException
    {
        ColPermsDescriptor colPerms = (ColPermsDescriptor) perm;
        FormatableBitSet existingColSet = (FormatableBitSet) row.getColumn( COLUMNS_COL_NUM).getObject();
        FormatableBitSet newColSet = colPerms.getColumns();

        boolean changed = false;
        for( int i = newColSet.anySetBit(); i >= 0; i = newColSet.anySetBit(i))
        {
            if( ! existingColSet.get(i))
            {
                existingColSet.set( i);
                changed = true;
            }
        }
        if( changed)
        {
            colsChanged[ COLUMNS_COL_NUM - 1] = true;
            return 1;
        }
        return 0;
    } // end of orPermissions

    /**
     * Remove a set of permissions from a row from this catalog table
     *
     * @param row an existing row
     * @param perm a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     * @param colsChanged An array with one element for each column in row. It is updated to
     *                    indicate which columns in row were changed
     *
     * @return -1 if there are no permissions left in the row, otherwise the number of columns that were changed.
     *
     * @exception StandardException standard error policy
     */
    public int removePermissions( ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
        throws StandardException
    {
        ColPermsDescriptor colPerms = (ColPermsDescriptor) perm;
        FormatableBitSet removeColSet = colPerms.getColumns();
        if( removeColSet == null)
            // remove all of them
            return -1;
        
        FormatableBitSet existingColSet = (FormatableBitSet) row.getColumn( COLUMNS_COL_NUM).getObject();

        boolean changed = false;
        for( int i = removeColSet.anySetBit(); i >= 0; i = removeColSet.anySetBit(i))
        {
            if( existingColSet.get(i))
            {
                existingColSet.clear( i);
                changed = true;
            }
        }
        if( changed)
        {
            colsChanged[ COLUMNS_COL_NUM - 1] = true;
            if( existingColSet.anySetBit() < 0)
                return -1; // No column privileges left
            return 1; // A change, but there are some privileges left
        }
        return 0; // no change
    } // end of removePermissions
    
	/** 
	 * @see PermissionsCatalogRowFactory#setUUIDOfThePassedDescriptor
	 */
    public void setUUIDOfThePassedDescriptor(ExecRow row, PermissionsDescriptor perm)
    throws StandardException
    {
        DataValueDescriptor existingPermDVD = row.getColumn(COLPERMSID_COL_NUM);
        perm.setUUID(getUUIDFactory().recreateUUID(existingPermDVD.getString()));
    }

    public List<ColumnDescriptor[]> getViewColumns(TableDescriptor view, UUID viewId) throws StandardException {

        List<ColumnDescriptor[]> cdsl = new ArrayList<>();
        cdsl.add(
                new ColumnDescriptor[]{
                        new ColumnDescriptor("COLPERMSID",1,1, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("GRANTEE",2,2,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("GRANTOR",3,3,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("TABLEID",4,4,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("TYPE",5,5,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("COLUMNS",6,6,
                                new DataTypeDescriptor(TypeId.getUserDefinedTypeId("com.splicemachine.db.iapi.services.io.FormatableBitSet", false), false),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("TABLENAME"               ,7,7,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("SCHEMAID"               ,8,8,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("SCHEMANAME"               ,9,9,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0)
                });
        return cdsl;
    }
    public static String SYSCOLPERMS_VIEW_SQL = "create view SYSCOLPERMSVIEW as \n" +
            "SELECT P.*, T.TABLENAME, T.SCHEMAID, T.SCHEMANAME FROM SYS.SYSCOLPERMS P, SYSVW.SYSTABLESVIEW T "+
            "WHERE T.TABLEID = P.TABLEID AND " +
            "P.grantee in (select name from sysvw.sysallroles) \n" +

            "UNION ALL \n" +

            "SELECT P.*, T.TABLENAME, T.SCHEMAID, T.SCHEMANAME FROM SYS.SYSCOLPERMS P, SYSVW.SYSTABLESVIEW T "+
            "WHERE T.TABLEID = P.TABLEID AND " +
            "'SPLICE' = (select name from new com.splicemachine.derby.vti.SpliceGroupUserVTI(2) as b (NAME VARCHAR(128)))";
}
