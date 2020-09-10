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

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLChar;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for creating a SYSTABLEPERMS row.
 *
 */

public class SYSTABLEPERMSRowFactory extends PermissionsCatalogRowFactory
{
	private static final String TABLENAME_STRING = "SYSTABLEPERMS";

    // Column numbers for the SYSTABLEPERMS table. 1 based
	private static final int TABLEPERMSID_COL_NUM = 1;
    private static final int GRANTEE_COL_NUM = 2;
    private static final int GRANTOR_COL_NUM = 3;
    private static final int TABLEID_COL_NUM = 4;
    private static final int SELECTPRIV_COL_NUM = 5;
    private static final int DELETEPRIV_COL_NUM = 6;
    private static final int INSERTPRIV_COL_NUM = 7;
    private static final int UPDATEPRIV_COL_NUM = 8;
    private static final int REFERENCESPRIV_COL_NUM = 9;
    private static final int TRIGGERPRIV_COL_NUM = 10;
    private static final int COLUMN_COUNT = 10;

    public static final int GRANTEE_TABLE_GRANTOR_INDEX_NUM = 0;
    public static final int TABLEPERMSID_INDEX_NUM = 1;
    public static final int TABLEID_INDEX_NUM = 2;
	private static final int[][] indexColumnPositions = 
	{ 
		{ GRANTEE_COL_NUM, TABLEID_COL_NUM, GRANTOR_COL_NUM},
		{ TABLEPERMSID_COL_NUM },
		{ TABLEID_COL_NUM }
	};

    public static final int GRANTEE_COL_NUM_IN_GRANTEE_TABLE_GRANTOR_INDEX = 1;

    private static final boolean[] indexUniqueness = { true, true, false};
    
    private	static final String[] uuids =
    {
        "b8450018-0103-0e39-b8e7-00000010f010" // catalog UUID
		,"004b0019-0103-0e39-b8e7-00000010f010"	// heap UUID
		,"c851401a-0103-0e39-b8e7-00000010f010"	// index1
		,"80220011-010c-426e-c599-0000000f1120"	// index2
		,"f81e0010-010c-bc85-060d-000000109ab8"	// index3
    };

    public SYSTABLEPERMSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf, DataDictionary dd)
	{
		super(uuidf,ef,dvf,dd);
		initInfo(COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, indexUniqueness, uuids);
	}

	public ExecRow makeRow(boolean latestVersion, TupleDescriptor td, TupleDescriptor parent) throws StandardException
	{
		UUID						oid;
        DataValueDescriptor grantee = null;
        DataValueDescriptor grantor = null;
        String tablePermID = null;
        String tableID = null;
        String selectPriv = null;
        String deletePriv = null;
        String insertPriv = null;
        String updatePriv = null;
        String referencesPriv = null;
        String triggerPriv = null;

        if( td == null)
        {
            grantee = getNullAuthorizationID();
            grantor = getNullAuthorizationID();
        }
        else
        {
            if (!(td instanceof TablePermsDescriptor))
                throw new RuntimeException("Unexpected TupleDescriptor " + td.getClass().getName());

            TablePermsDescriptor tpd = (TablePermsDescriptor) td;
            oid = tpd.getUUID();
            if ( oid == null )
            {
				oid = getUUIDFactory().createUUID();
				tpd.setUUID(oid);
            }
            tablePermID = oid.toString();

			grantee = getAuthorizationID( tpd.getGrantee());
            grantor = getAuthorizationID( tpd.getGrantor());
            tableID = tpd.getTableUUID().toString();
            selectPriv = tpd.getSelectPriv();
            deletePriv = tpd.getDeletePriv();
            insertPriv = tpd.getInsertPriv();
            updatePriv = tpd.getUpdatePriv();
            referencesPriv = tpd.getReferencesPriv();
            triggerPriv = tpd.getTriggerPriv();
        }
        ExecRow row = getExecutionFactory().getValueRow( COLUMN_COUNT);
        row.setColumn( TABLEPERMSID_COL_NUM, new SQLChar(tablePermID));
        row.setColumn( GRANTEE_COL_NUM, grantee);
        row.setColumn( GRANTOR_COL_NUM, grantor);
        row.setColumn( TABLEID_COL_NUM, new SQLChar(tableID));
        row.setColumn( SELECTPRIV_COL_NUM, new SQLChar(selectPriv));
        row.setColumn( DELETEPRIV_COL_NUM, new SQLChar(deletePriv));
        row.setColumn( INSERTPRIV_COL_NUM, new SQLChar(insertPriv));
        row.setColumn( UPDATEPRIV_COL_NUM, new SQLChar(updatePriv));
        row.setColumn( REFERENCESPRIV_COL_NUM, new SQLChar( referencesPriv));
        row.setColumn( TRIGGERPRIV_COL_NUM, new SQLChar(triggerPriv));

        return row;
    } // end of makeRow
            
	/** builds a tuple descriptor from a row */
	public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary)
		throws StandardException
    {
		if( SanityManager.DEBUG)
            SanityManager.ASSERT( row.nColumns() == COLUMN_COUNT,
                                  "Wrong size row passed to SYSTABLEPERMSRowFactory.buildDescriptor");

        String tablePermsUUIDString = row.getColumn(TABLEPERMSID_COL_NUM).getString();
        UUID tablePermsUUID = getUUIDFactory().recreateUUID(tablePermsUUIDString);
        String tableUUIDString = row.getColumn( TABLEID_COL_NUM).getString();
        UUID tableUUID = getUUIDFactory().recreateUUID(tableUUIDString);
        String selectPriv  = row.getColumn( SELECTPRIV_COL_NUM).getString();
        String deletePriv  = row.getColumn( DELETEPRIV_COL_NUM).getString();
        String insertPriv  = row.getColumn( INSERTPRIV_COL_NUM).getString();
        String updatePriv  = row.getColumn( UPDATEPRIV_COL_NUM).getString();
        String referencesPriv  = row.getColumn( REFERENCESPRIV_COL_NUM).getString();
        String triggerPriv  = row.getColumn( TRIGGERPRIV_COL_NUM).getString();
        if( SanityManager.DEBUG)
        {
            SanityManager.ASSERT( "y".equals(selectPriv) || "Y".equals(selectPriv) || "N".equals(selectPriv),
                                  "Invalid SYSTABLEPERMS.selectPriv column value: " + selectPriv);
            SanityManager.ASSERT( "y".equals(deletePriv) || "Y".equals(deletePriv) || "N".equals(deletePriv),
                                  "Invalid SYSTABLEPERMS.deletePriv column value: " + deletePriv);
            SanityManager.ASSERT( "y".equals(insertPriv) || "Y".equals(insertPriv) || "N".equals(insertPriv),
                                  "Invalid SYSTABLEPERMS.insertPriv column value: " + insertPriv);
            SanityManager.ASSERT( "y".equals(updatePriv) || "Y".equals(updatePriv) || "N".equals(updatePriv),
                                  "Invalid SYSTABLEPERMS.updatePriv column value: " + updatePriv);
            SanityManager.ASSERT( "y".equals(referencesPriv) || "Y".equals(referencesPriv) || "N".equals(referencesPriv),
                                  "Invalid SYSTABLEPERMS.referencesPriv column value: " + referencesPriv);
            SanityManager.ASSERT( "y".equals(triggerPriv) || "Y".equals(triggerPriv) || "N".equals(triggerPriv),
                                  "Invalid SYSTABLEPERMS.triggerPriv column value: " + triggerPriv);
        }

		TablePermsDescriptor tabPermsDesc =
        new TablePermsDescriptor( dataDictionary,
                                         getAuthorizationID( row, GRANTEE_COL_NUM),
                                         getAuthorizationID( row, GRANTOR_COL_NUM),
                                         tableUUID,
                                         selectPriv, deletePriv, insertPriv,
                                         updatePriv, referencesPriv, triggerPriv);
		tabPermsDesc.setUUID(tablePermsUUID);
		return tabPermsDesc;
    } // end of buildDescriptor

	/** builds a column list for the catalog */
	public SystemColumn[] buildColumnList()
        throws StandardException
    {
        return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("TABLEPERMSID", false),
            SystemColumnImpl.getIdentifierColumn("GRANTEE", false),
            SystemColumnImpl.getIdentifierColumn("GRANTOR", false),
            SystemColumnImpl.getUUIDColumn("TABLEID", false),
            SystemColumnImpl.getIndicatorColumn("SELECTPRIV"),
            SystemColumnImpl.getIndicatorColumn("DELETEPRIV"),
            SystemColumnImpl.getIndicatorColumn("INSERTPRIV"),
            SystemColumnImpl.getIndicatorColumn("UPDATEPRIV"),
            SystemColumnImpl.getIndicatorColumn("REFERENCESPRIV"),
            SystemColumnImpl.getIndicatorColumn("TRIGGERPRIV"),
        };
    }

	/**
	 * builds a key row given for a given index number.
	 */
  	public ExecIndexRow buildIndexKeyRow( int indexNumber,
                                          PermissionsDescriptor perm) 
  		throws StandardException
    {
        ExecIndexRow row = null;
        
        switch( indexNumber)
        {
            case GRANTEE_TABLE_GRANTOR_INDEX_NUM:
                // RESOLVE We do not support the FOR GRANT OPTION, so table permission rows are unique on the
                // grantee and table UUID columns. The grantor column will always have the name of the owner of the
                // table. So the index key, used for searching the index, only has grantee and table UUID columns.
                // It does not have a grantor column.
                //
                // If we support FOR GRANT OPTION then there may be multiple table permissions rows for a
                // (grantee, tableID) combination. We must either handle the multiple rows, which is necessary for
                // checking permissions, or add a grantor column to the key, which is necessary for granting or revoking
                // permissions.
                row = getExecutionFactory().getIndexableRow( 2);
                row.setColumn(1, getAuthorizationID( perm.getGrantee()));
                String tableUUIDStr = ((TablePermsDescriptor) perm).getTableUUID().toString();
                row.setColumn(2, new SQLChar(tableUUIDStr));
                break;
            case TABLEPERMSID_INDEX_NUM:
                row = getExecutionFactory().getIndexableRow( 1);
                String tablePermsUUIDStr = perm.getObjectID().toString();
                row.setColumn(1, new SQLChar(tablePermsUUIDStr));
                break;
            case TABLEID_INDEX_NUM:
                row = getExecutionFactory().getIndexableRow( 1);
                tableUUIDStr = ((TablePermsDescriptor) perm).getTableUUID().toString();
                row.setColumn(1, new SQLChar(tableUUIDStr));
                break;
            default:
                break;
        }
        return row;
    } // end of buildIndexRow
    
    public int getPrimaryKeyIndexNumber()
    {
        return GRANTEE_TABLE_GRANTOR_INDEX_NUM;
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
        TablePermsDescriptor tablePerms = (TablePermsDescriptor) perm;
        int changeCount = 0;
        changeCount += orOnePermission( row, colsChanged, SELECTPRIV_COL_NUM, tablePerms.getSelectPriv());
        changeCount += orOnePermission( row, colsChanged, DELETEPRIV_COL_NUM, tablePerms.getDeletePriv());
        changeCount += orOnePermission( row, colsChanged, INSERTPRIV_COL_NUM, tablePerms.getInsertPriv());
        changeCount += orOnePermission( row, colsChanged, UPDATEPRIV_COL_NUM, tablePerms.getUpdatePriv());
        changeCount += orOnePermission( row, colsChanged, REFERENCESPRIV_COL_NUM, tablePerms.getReferencesPriv());
        changeCount += orOnePermission( row, colsChanged, TRIGGERPRIV_COL_NUM, tablePerms.getTriggerPriv());

        return changeCount;
    } // end of orPermissions

    private int orOnePermission( ExecRow row, boolean[] colsChanged, int column, String permission)
        throws StandardException
    {
        if( permission.charAt(0) == 'N')
            return 0;

        if( SanityManager.DEBUG)
            SanityManager.ASSERT( permission.charAt(0) == 'Y' || permission.charAt(0) == 'y',
                                  "Invalid permission passed to SYSTABLEPERMSRowFactory.orOnePermission");
        DataValueDescriptor existingPermDVD = row.getColumn( column);
        char existingPerm = existingPermDVD.getString().charAt(0);
        if( existingPerm == 'Y' || existingPerm == permission.charAt(0))
            return 0;
        existingPermDVD.setValue( permission);
        colsChanged[ column - 1] = true;
        return 1;
    } // end of orOnePermission

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
        TablePermsDescriptor tablePerms = (TablePermsDescriptor) perm;
        int changeCount = 0;
        boolean permissionsLeft =
          ( removeOnePermission( row, colsChanged, SELECTPRIV_COL_NUM, tablePerms.getSelectPriv()) |
            removeOnePermission( row, colsChanged, DELETEPRIV_COL_NUM, tablePerms.getDeletePriv()) |
            removeOnePermission( row, colsChanged, INSERTPRIV_COL_NUM, tablePerms.getInsertPriv()) |
            removeOnePermission( row, colsChanged, UPDATEPRIV_COL_NUM, tablePerms.getUpdatePriv()) |
            removeOnePermission( row, colsChanged, REFERENCESPRIV_COL_NUM, tablePerms.getReferencesPriv()) |
            removeOnePermission( row, colsChanged, TRIGGERPRIV_COL_NUM, tablePerms.getTriggerPriv()));
        if( ! permissionsLeft)
            return -1;
        for (boolean aColsChanged : colsChanged) {
            if (aColsChanged)
                changeCount++;
        }
        return changeCount;
    } // end of removePermissions

    private boolean removeOnePermission( ExecRow row, boolean[] colsChanged, int column, String permission)
        throws StandardException
    {
        DataValueDescriptor existingPermDVD = row.getColumn( column);
        char existingPerm = existingPermDVD.getString().charAt(0);

        if( permission.charAt(0) == 'N') // Don't remove this one
            return existingPerm != 'N'; // The grantee still has some permissions on this table
        if( SanityManager.DEBUG)
            SanityManager.ASSERT( permission.charAt(0) == 'Y' || permission.charAt(0) == 'y',
                                  "Invalid permission passed to SYSTABLEPERMSRowFactory.removeOnePermission");
        if( existingPerm != 'N')
        {
            existingPermDVD.setValue( "N");
            colsChanged[ column - 1] = true;
        }
        return false;
    } // end of removeOnePermission
    
	/** 
	 * @see PermissionsCatalogRowFactory#setUUIDOfThePassedDescriptor
	 */
    public void setUUIDOfThePassedDescriptor(ExecRow row, PermissionsDescriptor perm)
    throws StandardException
    {
        DataValueDescriptor existingPermDVD = row.getColumn(TABLEPERMSID_COL_NUM);
        perm.setUUID(getUUIDFactory().recreateUUID(existingPermDVD.getString()));
    }

    public List<ColumnDescriptor[]> getViewColumns(TableDescriptor view, UUID viewId) throws StandardException {

        List<ColumnDescriptor[]> cdsl = new ArrayList<>();
        cdsl.add(
                new ColumnDescriptor[]{
                        new ColumnDescriptor("TABLEPERMSID",1,1, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
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
                        new ColumnDescriptor("SELECTPRIV",5,5,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("DELETEPRIV",6,6,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("INSERTPRIV",7,7,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("UPDATEPRIV",8,8,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("REFERENCESPRIV",9,9,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("TRIGGERPRIV",10,10,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("TABLENAME"               ,11,11,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("SCHEMAID"               ,12,12,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("SCHEMANAME"               ,13,13,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0)
                });
        return cdsl;
    }
    public static String SYSTABLEPERMS_VIEW_SQL = "create view SYSTABLEPERMSVIEW as \n" +
            "SELECT P.*, T.TABLENAME, T.SCHEMAID, T.SCHEMANAME FROM SYS.SYSTABLEPERMS P, SYSVW.SYSTABLESVIEW T "+
            "WHERE T.TABLEID = P.TABLEID AND " +
            "P.grantee in (select name from sysvw.sysallroles) \n" +

            "UNION ALL \n" +

            "SELECT P.*, T.TABLENAME, T.SCHEMAID, T.SCHEMANAME FROM SYS.SYSTABLEPERMS P, SYSVW.SYSTABLESVIEW T "+
            "WHERE T.TABLEID = P.TABLEID AND " +
            "'SPLICE' = (select name from new com.splicemachine.derby.vti.SpliceGroupUserVTI(2) as b (NAME VARCHAR(128)))";
}
