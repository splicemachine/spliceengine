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

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for creating a SYSPERMS row.
 */

public class SYSPERMSRowFactory extends PermissionsCatalogRowFactory {
    private static final String TABLENAME_STRING = "SYSPERMS";

    private static final int SYSPERMS_COLUMN_COUNT = 7;
    /* Column #s for sysinfo (1 based) */
    private static final int SYSPERMS_PERMISSIONID = 1;
    private static final int SYSPERMS_OBJECTTYPE = 2;
    private static final int SYSPERMS_OBJECTID = 3;
    private static final int SYSPERMS_PERMISSION = 4;
    private static final int SYSPERMS_GRANTOR = 5;
    private static final int SYSPERMS_GRANTEE = 6;
    private static final int SYSPERMS_IS_GRANTABLE = 7;

    private static final int[][] indexColumnPositions =
            {
                    {SYSPERMS_PERMISSIONID},
                    {SYSPERMS_OBJECTID},
                    {SYSPERMS_GRANTEE, SYSPERMS_OBJECTID, SYSPERMS_GRANTOR},
            };

    // index numbers
    public static final int PERMS_UUID_IDX_NUM = 0;
    public static final int PERMS_OBJECTID_IDX_NUM = 1;
    public static final int GRANTEE_OBJECTID_GRANTOR_INDEX_NUM = 2;

    public static final int GRANTEE_COL_NUM_IN_GRANTEE_OBJECTID_GRANTOR_INDEX = 1;

    private static final boolean[] uniqueness = { true, false, true };

    private static final String[] uuids = {
            "9810800c-0121-c5e1-a2f5-00000043e718", // catalog UUID
            "6ea6ffac-0121-c5e3-f286-00000043e718", // heap UUID
            "5cc556fc-0121-c5e6-4e43-00000043e718",  // PERMS_UUID_IDX_NUM
            "7a92cf84-0122-51e6-2c5e-00000047b548",   // PERMS_OBJECTID_IDX_NUM
            "9810800c-0125-8de5-3aa0-0000001999e8",   // GRANTEE_OBJECTID_GRANTOR_INDEX_NUM
    };



    /**
     * Constructor
     *
     * @param uuidf UUIDFactory
     * @param ef    ExecutionFactory
     * @param dvf   DataValueFactory
     */
    public SYSPERMSRowFactory(UUIDFactory uuidf,
                       ExecutionFactory ef,
                       DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(SYSPERMS_COLUMN_COUNT, TABLENAME_STRING,
                indexColumnPositions, uniqueness, uuids);
    }

    /**
     * builds an index key row given for a given index number.
     */
    public ExecIndexRow buildIndexKeyRow(int indexNumber,
                                         PermissionsDescriptor perm)
            throws StandardException {
        ExecIndexRow row = null;

        switch (indexNumber) {
        case GRANTEE_OBJECTID_GRANTOR_INDEX_NUM:
            // RESOLVE We do not support the FOR GRANT OPTION, so generic permission rows are unique on the
            // grantee and object UUID columns. The grantor column will always have the name of the owner of the
            // object. So the index key, used for searching the index, only has grantee and object UUID columns.
            // It does not have a grantor column.
            row = getExecutionFactory().getIndexableRow( 2 );
            row.setColumn(1, getAuthorizationID( perm.getGrantee()));
            String protectedObjectsIDStr = ((PermDescriptor) perm).getPermObjectId().toString();
            row.setColumn(2, new SQLChar(protectedObjectsIDStr));
            break;

        case PERMS_UUID_IDX_NUM:
                row = getExecutionFactory().getIndexableRow(1);
                String permUUIDStr = ((PermDescriptor) perm).getUUID().toString();
                row.setColumn(1, new SQLChar(permUUIDStr));
                break;
        }
        return row;
    } // end of buildIndexKeyRow

    public int getPrimaryKeyIndexNumber()
    {
        return GRANTEE_OBJECTID_GRANTOR_INDEX_NUM;
    }

    /**
     * Or a set of permissions in with a row from this catalog table
     *
     * @param row         an existing row
     * @param perm        a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     * @param colsChanged An array with one element for each column in row. It is updated to
     *                    indicate which columns in row were changed
     * @return The number of columns that were changed.
     * @throws StandardException standard error policy
     */
    public int orPermissions(ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
            throws StandardException {
        return 0;
    }

    /**
     * Remove a set of permissions from a row from this catalog table
     *
     * @param row         an existing row
     * @param perm        a permission descriptor of the appropriate class for this PermissionsCatalogRowFactory class.
     * @param colsChanged An array with one element for each column in row. It is updated to
     *                    indicate which columns in row were changed
     * @return -1 if there are no permissions left in the row, otherwise the number of columns that were changed.
     * @throws StandardException standard error policy
     */
    public int removePermissions(ExecRow row, PermissionsDescriptor perm, boolean[] colsChanged)
            throws StandardException {
        return -1; // There is only one kind of privilege per row so delete the whole row.
    } // end of removePermissions

	/** 
	 * @see PermissionsCatalogRowFactory#setUUIDOfThePassedDescriptor
	 */
    void setUUIDOfThePassedDescriptor(ExecRow row, PermissionsDescriptor perm) throws StandardException
    {
        DataValueDescriptor existingPermDVD = row.getColumn(SYSPERMS_PERMISSIONID);
        perm.setUUID(getUUIDFactory().recreateUUID(existingPermDVD.getString()));
    }

    /**
     * Make a SYSPERMS row
     *
     * @param td     a permission descriptor
     * @param parent unused
     * @return Row suitable for inserting into SYSPERMS.
     * @throws com.splicemachine.db.iapi.error.StandardException
     *          thrown on failure
     */
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
            throws StandardException {
        ExecRow row;
        String permIdString = null;
        String objectType = "SEQUENCE";
        String objectIdString = null;
        String permission = "USAGE";
        String grantor = null;
        String grantee = null;
        boolean grantable = false;


        if (td != null) {
            PermDescriptor sd = (PermDescriptor) td;
            UUID pid = sd.getUUID();
            if ( pid == null )
            {
				pid = getUUIDFactory().createUUID();
				sd.setUUID(pid);
            }
            permIdString = pid.toString();

            objectType = sd.getObjectType();

            UUID oid = sd.getPermObjectId();
            objectIdString = oid.toString();

            permission = sd.getPermission();
            grantor = sd.getGrantor();
            grantee = sd.getGrantee();
            grantable = sd.isGrantable();
        }

        /* Build the row to insert */
        row = getExecutionFactory().getValueRow(SYSPERMS_COLUMN_COUNT);

        /* 1st column is UUID */
        row.setColumn(1, new SQLChar(permIdString));

        /* 2nd column is OBJECTTYPE */
        row.setColumn(2, new SQLVarchar(objectType));

        /* 3rd column is OBJECTID */
        row.setColumn(3, new SQLChar(objectIdString));

        /* 4nd column is OBJECTTYPE */
        row.setColumn(4, new SQLChar(permission));

        /* 5nd column is GRANTOR */
        row.setColumn(5, new SQLVarchar(grantor));

        /* 6nd column is GRANTEE */
        row.setColumn(6, new SQLVarchar(grantee));

        /* 7nd column is IS_GRANTABLE */
        row.setColumn(7, new SQLChar(grantable ? "Y" : "N"));

        return row;
    }

    ///////////////////////////////////////////////////////////////////////////
    //
    //  ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
    //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Make an  Tuple Descriptor out of a SYSPERMS row
     *
     * @param row                   a SYSPERMS row
     * @param parentTupleDescriptor unused
     * @param dd                    dataDictionary
     * @return a  descriptor equivalent to a SYSPERMS row
     * @throws com.splicemachine.db.iapi.error.StandardException
     *          thrown on failure
     */
    public TupleDescriptor buildDescriptor
            (ExecRow row,
             TupleDescriptor parentTupleDescriptor,
             DataDictionary dd)
            throws StandardException {

        DataValueDescriptor col;
        PermDescriptor descriptor;
        String permIdString;
        String objectType;
        String objectIdString;
        String permission;
        String grantor;
        String grantee;
        String isGrantable;

        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(row.nColumns() == SYSPERMS_COLUMN_COUNT,
                    "Wrong number of columns for a SYSPERMS row");
        }

        // first column is uuid of this permission descriptor (char(36))
        col = row.getColumn(SYSPERMS_PERMISSIONID);
        permIdString = col.getString();

        // second column is objectType (varchar(36))
        col = row.getColumn(SYSPERMS_OBJECTTYPE);
        objectType = col.getString();

        // third column is objectid (varchar(36))
        col = row.getColumn(SYSPERMS_OBJECTID);
        objectIdString = col.getString();

        // fourth column is permission (varchar(128))
        col = row.getColumn(SYSPERMS_PERMISSION);
        permission = col.getString();

        // fifth column is grantor auth Id (varchar(128))
        col = row.getColumn(SYSPERMS_GRANTOR);
        grantor = col.getString();

        // sixth column is grantee auth Id (varchar(128))
        col = row.getColumn(SYSPERMS_GRANTEE);
        grantee = col.getString();

        // seventh column is isGrantable (char(1))
        col = row.getColumn(SYSPERMS_IS_GRANTABLE);
        isGrantable = col.getString();

        descriptor = ddg.newPermDescriptor
                (getUUIDFactory().recreateUUID(permIdString),
                        objectType,
                        getUUIDFactory().recreateUUID(objectIdString),
                        permission,
                        grantor,
                        grantee,
                        isGrantable.equals("Y"));

        return descriptor;
    }

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[] buildColumnList()
        throws StandardException
    {
        return new SystemColumn[]{
                SystemColumnImpl.getUUIDColumn("UUID", false),
                SystemColumnImpl.getColumn("OBJECTTYPE", Types.VARCHAR, false, 36),
                SystemColumnImpl.getUUIDColumn("OBJECTID", false),
                SystemColumnImpl.getColumn("PERMISSION", Types.CHAR, false, 36),
                SystemColumnImpl.getIdentifierColumn("GRANTOR", false),
                SystemColumnImpl.getIdentifierColumn("GRANTEE", false),
                SystemColumnImpl.getIndicatorColumn("ISGRANTABLE")
        };
    }

    public List<ColumnDescriptor[]> getViewColumns(TableDescriptor view, UUID viewId) throws StandardException {

        List<ColumnDescriptor[]> cdsl = new ArrayList<>();
        cdsl.add(
                new ColumnDescriptor[]{
                        new ColumnDescriptor("UUID",1,1, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("OBJECTTYPE",2,2,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 36),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("OBJECTID",3,3, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("PERMISSION",4,4, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("GRANTOR",5,5,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("GRANTEE",6,6,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("ISGRANTABLE",7,7,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("OBJECTNAME",8,8,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0),
                        new ColumnDescriptor("SCHEMANAME",9,9,
                                DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128),
                                null,null,view,viewId,0,0,0)
                });
        return cdsl;
    }
    public static String SYSPERMS_VIEW_SQL = "create view SYSPERMSVIEW as \n" +
            "SELECT P.*, case when OBJECTTYPE='SEQUENCE' then SE.SEQUENCENAME else A.ALIAS end as OBJECTNAME, SC.SCHEMANAME " +
            "FROM SYS.SYSPERMS P left join SYS.SYSALIASES A on P.OBJECTID=A.ALIASID " +
            "                    left join SYS.SYSSEQUENCES SE on P.OBJECTID=SE.SEQUENCEID " +
            ", SYS.SYSSCHEMAS SC "+
            "WHERE case when OBJECTTYPE='SEQUENCE' then SE.SCHEMAID else A.SCHEMAID end = SC.SCHEMAID AND " +
            "P.grantee in (select name from sysvw.sysallroles) \n " +

            "UNION ALL \n" +

            "SELECT P.*, case when OBJECTTYPE='SEQUENCE' then SE.SEQUENCENAME else A.ALIAS end as OBJECTNAME, SC.SCHEMANAME " +
            "FROM SYS.SYSPERMS P left join SYS.SYSALIASES A on P.OBJECTID=A.ALIASID " +
            "                    left join SYS.SYSSEQUENCES SE on P.OBJECTID=SE.SEQUENCEID " +
            ", SYS.SYSSCHEMAS SC "+
            "WHERE case when OBJECTTYPE='SEQUENCE' then SE.SCHEMAID else A.SCHEMAID end = SC.SCHEMAID AND " +
            "'SPLICE' = (select name from new com.splicemachine.derby.vti.SpliceGroupUserVTI(2) as b (NAME VARCHAR(128)))";
}
