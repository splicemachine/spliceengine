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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.sql.dictionary.*;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.catalog.UUID;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for creating a SYSROUTINEPERMS row.
 *
 */

public class SYSROUTINEPERMSRowFactory extends PermissionsCatalogRowFactory
{
	static final String TABLENAME_STRING = "SYSROUTINEPERMS";

    // Column numbers for the SYSROUTINEPERMS table. 1 based
    public static final int ROUTINEPERMSID_COL_NUM = 1;
    private static final int GRANTEE_COL_NUM = 2;
    private static final int GRANTOR_COL_NUM = 3;
    public static final int ALIASID_COL_NUM = 4;
    private static final int GRANTOPTION_COL_NUM = 5;
    private static final int COLUMN_COUNT = 5;

    static final int GRANTEE_ALIAS_GRANTOR_INDEX_NUM = 0;
    public static final int ROUTINEPERMSID_INDEX_NUM = 1;
    public static final int ALIASID_INDEX_NUM = 2;

	private static final int[][] indexColumnPositions = 
	{ 
		{ GRANTEE_COL_NUM, ALIASID_COL_NUM, GRANTOR_COL_NUM},
		{ ROUTINEPERMSID_COL_NUM },
		{ ALIASID_COL_NUM }
	};

    public static final int GRANTEE_COL_NUM_IN_GRANTEE_ALIAS_GRANTOR_INDEX = 1;

    private static final boolean[] indexUniqueness = { true, true, false };

    private	static final String[] uuids =
    {
        "2057c01b-0103-0e39-b8e7-00000010f010" // catalog UUID
		,"185e801c-0103-0e39-b8e7-00000010f010"	// heap UUID
		,"c065801d-0103-0e39-b8e7-00000010f010"	// index1
		,"40f70088-010c-4c2f-c8de-0000000f43a0" // index2
		,"08264012-010c-bc85-060d-000000109ab8" // index3
    };

    public SYSROUTINEPERMSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo( COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, indexUniqueness, uuids);
	}

	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException
	{
		UUID oid;
        String routinePermID = null;
        DataValueDescriptor grantee = null;
        DataValueDescriptor grantor = null;
        String routineID = null;
        
        if( td == null)
        {
            grantee = getNullAuthorizationID();
            grantor = getNullAuthorizationID();
        }
        else
        {
            RoutinePermsDescriptor rpd = (RoutinePermsDescriptor) td;
            oid = rpd.getUUID();
            if ( oid == null )
            {
				oid = getUUIDFactory().createUUID();
				rpd.setUUID(oid);
            }
            routinePermID = oid.toString();
            grantee = getAuthorizationID( rpd.getGrantee());
            grantor = getAuthorizationID( rpd.getGrantor());
            if( rpd.getRoutineUUID() != null)
                routineID = rpd.getRoutineUUID().toString();
        }
		ExecRow row = getExecutionFactory().getValueRow( COLUMN_COUNT);
		row.setColumn( ROUTINEPERMSID_COL_NUM, new SQLChar(routinePermID));
        row.setColumn( GRANTEE_COL_NUM, grantee);
        row.setColumn( GRANTOR_COL_NUM, grantor);
        row.setColumn( ALIASID_COL_NUM, new SQLChar(routineID));
        row.setColumn( GRANTOPTION_COL_NUM, new SQLChar("N"));
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
                                  "Wrong size row passed to SYSROUTINEPERMSRowFactory.buildDescriptor");

        String routinePermsUUIDString = row.getColumn(ROUTINEPERMSID_COL_NUM).getString();
        UUID routinePermsUUID = getUUIDFactory().recreateUUID(routinePermsUUIDString);
        String aliasUUIDString = row.getColumn( ALIASID_COL_NUM).getString();
        UUID aliasUUID = getUUIDFactory().recreateUUID(aliasUUIDString);

        RoutinePermsDescriptor routinePermsDesc =
	        new RoutinePermsDescriptor( dataDictionary,
                    getAuthorizationID( row, GRANTEE_COL_NUM),
                    getAuthorizationID( row, GRANTOR_COL_NUM),
                    aliasUUID);
        routinePermsDesc.setUUID(routinePermsUUID);
			return routinePermsDesc;
    } // end of buildDescriptor

	/** builds a column list for the catalog */
	public SystemColumn[] buildColumnList()
        throws StandardException
    {
         return new SystemColumn[] {
             SystemColumnImpl.getUUIDColumn("ROUTINEPERMSID", false),
             SystemColumnImpl.getIdentifierColumn("GRANTEE", false),
             SystemColumnImpl.getIdentifierColumn("GRANTOR", false),
             SystemColumnImpl.getUUIDColumn("ALIASID", false),
             SystemColumnImpl.getIndicatorColumn("GRANTOPTION")
         };
    }

	/**
	 * builds an index key row given for a given index number.
	 */
  	public ExecIndexRow buildIndexKeyRow( int indexNumber,
                                          PermissionsDescriptor perm) 
  		throws StandardException
    {
        ExecIndexRow row = null;
        
        switch( indexNumber)
        {
        case GRANTEE_ALIAS_GRANTOR_INDEX_NUM:
            // RESOLVE We do not support the FOR GRANT OPTION, so rougine permission rows are unique on the
            // grantee and alias UUID columns. The grantor column will always have the name of the owner of the
            // routine. So the index key, used for searching the index, only has grantee and alias UUID columns.
            // It does not have a grantor column.
            //
            // If we support FOR GRANT OPTION then there may be multiple routine permissions rows for a
            // (grantee, aliasID) combination. Since there is only one kind of routine permission (execute)
            // execute permission checking need not worry about multiple routine permission rows for a
            // (grantee, aliasID) combination, it only cares whether there are any. Grant and revoke must
            // look through multiple rows to see if the current user has grant/revoke permission and use
            // the full key in checking for the pre-existence of the permission being granted or revoked.
            row = getExecutionFactory().getIndexableRow( 2);
            row.setColumn(1, getAuthorizationID( perm.getGrantee()));
            String routineUUIDStr = ((RoutinePermsDescriptor) perm).getRoutineUUID().toString();
            row.setColumn(2, new SQLChar(routineUUIDStr));
            break;
        case ROUTINEPERMSID_INDEX_NUM:
            row = getExecutionFactory().getIndexableRow( 1);
            String routinePermsUUIDStr = perm.getObjectID().toString();
            row.setColumn(1, new SQLChar(routinePermsUUIDStr));
            break;
        case ALIASID_INDEX_NUM:
            row = getExecutionFactory().getIndexableRow( 1);
            routineUUIDStr = ((RoutinePermsDescriptor) perm).getRoutineUUID().toString();
            row.setColumn(1, new SQLChar(routineUUIDStr));
            break;
        }
        return row;
    } // end of buildIndexKeyRow
    
    public int getPrimaryKeyIndexNumber()
    {
        return GRANTEE_ALIAS_GRANTOR_INDEX_NUM;
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
        // There is only one kind of routine permission: execute or not. So the row would not exist
        // unless execute permission is there.
        // This changes if we implement WITH GRANT OPTION.
        return 0;
    }

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
        return -1; // There is only one kind of routine privilege so delete the whole row.
    } // end of removePermissions
    
	/** 
	 * @see PermissionsCatalogRowFactory#setUUIDOfThePassedDescriptor
	 */
    public void setUUIDOfThePassedDescriptor(ExecRow row, PermissionsDescriptor perm)
    throws StandardException
    {
        DataValueDescriptor existingPermDVD = row.getColumn(ROUTINEPERMSID_COL_NUM);
        perm.setUUID(getUUIDFactory().recreateUUID(existingPermDVD.getString()));
    }

    public List<ColumnDescriptor[]> getViewColumns(TableDescriptor view, UUID viewId) throws StandardException {

        List<ColumnDescriptor[]> cdsl = new ArrayList<>();
        cdsl.add(
                new ColumnDescriptor[]{
                        new ColumnDescriptor("ROUTINEPERMSID",1,1, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("GRANTEE",2,2,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("GRANTOR",3,3,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("ALIASID",4,4,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("GRANTOPTION",5,5,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("ALIAS",6,6,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("SCHEMANAME",7,7,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0)
                });
        return cdsl;
    }
    public static String SYSROUTINEPERMS_VIEW_SQL = "create view SYSROUTINEPERMSVIEW as \n" +
            "SELECT P.*, A.ALIAS, S.SCHEMANAME FROM SYS.SYSROUTINEPERMS P, SYS.SYSALIASES A, SYS.SYSSCHEMAS S "+
            "WHERE P.ALIASID = A.ALIASID AND A.SCHEMAID= S.SCHEMAID AND " +
            "P.grantee in (select name from sysvw.sysallroles) \n" +

            "UNION ALL \n" +

            "SELECT P.*, A.ALIAS, S.SCHEMANAME FROM SYS.SYSROUTINEPERMS P, SYS.SYSALIASES A, SYS.SYSSCHEMAS S "+
            "WHERE P.ALIASID = A.ALIASID AND A.SCHEMAID= S.SCHEMAID AND " +
            "'SPLICE' = (select name from new com.splicemachine.derby.vti.SpliceGroupUserVTI(2) as b (NAME VARCHAR(128)))";
}
