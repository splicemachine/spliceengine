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
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.*;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for creating a SYSSCHEMAS row.
 *
 *
 * @version 0.1
 */

public class SYSSCHEMASRowFactory extends CatalogRowFactory
{
    public static    final    String    TABLENAME_STRING = "SYSSCHEMAS";

    public static final int SYSSCHEMAS_COLUMN_COUNT = 4;
    /* Column #s for sysinfo (1 based) */
    public static final int SYSSCHEMAS_SCHEMAID = 1;
    public static final int SYSSCHEMAS_SCHEMANAME = 2;
    public static final int SYSSCHEMAS_SCHEMAAID = 3;
    public static final int SYSSCHEMAS_DATABASEID = 4;


    public static final int SYSSCHEMAS_INDEX1_ID = 0;
    protected static final int SYSSCHEMAS_INDEX1_DATABASEID = 1;
    protected static final int SYSSCHEMAS_INDEX1_SCHEMANAME = 2;

    public static final int SYSSCHEMAS_INDEX2_ID = 1;


    private static final int[][] indexColumnPositions =
    {
        {SYSSCHEMAS_DATABASEID, SYSSCHEMAS_SCHEMANAME},
        {SYSSCHEMAS_SCHEMAID}
    };

    private    static    final    boolean[]    uniqueness = null;

    private    static    final    String[]    uuids =
    {
         "80000022-00d0-fd77-3ed8-000a0a0b1900"    // catalog UUID
        ,"8000002a-00d0-fd77-3ed8-000a0a0b1900"    // heap UUID
        ,"80000024-00d0-fd77-3ed8-000a0a0b1900"    // SYSSCHEMAS_INDEX1
        ,"80000026-00d0-fd77-3ed8-000a0a0b1900"    // SYSSCHEMAS_INDEX2
    };

    /////////////////////////////////////////////////////////////////////////////
    //
    //    CONSTRUCTORS
    //
    /////////////////////////////////////////////////////////////////////////////

    SYSSCHEMASRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf, DataDictionary dd)
    {
        super(uuidf,ef,dvf, dd);
        initInfo(SYSSCHEMAS_COLUMN_COUNT, TABLENAME_STRING,
                indexColumnPositions, uniqueness, uuids );
    }

    SYSSCHEMASRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
    {
        super(uuidf,ef,dvf);
        initInfo(SYSSCHEMAS_COLUMN_COUNT, TABLENAME_STRING,
                 indexColumnPositions, uniqueness, uuids );
    }

    /////////////////////////////////////////////////////////////////////////////
    //
    //    METHODS
    //
    /////////////////////////////////////////////////////////////////////////////

  /**
     * Make a SYSSCHEMAS row
     *
     * @return    Row suitable for inserting into SYSSCHEMAS.
     *
     * @exception   StandardException thrown on failure
     */

    public ExecRow makeRow(boolean latestVersion, TupleDescriptor td, TupleDescriptor parent)
                    throws StandardException
    {
        DataTypeDescriptor  dtd;
        ExecRow             row;
        DataValueDescriptor col;
        String              name = null;
        UUID                oid = null;
        String              uuid = null;
        String              aid = null;
        String              dbid = null;

        if (td != null)
        {
            if (!(td instanceof SchemaDescriptor))
                throw new RuntimeException("Unexpected SchemaDescriptor " + td.getClass().getName());

            SchemaDescriptor    schemaDescriptor = (SchemaDescriptor)td;

            name = schemaDescriptor.getSchemaName();
            oid = schemaDescriptor.getUUID();
            if ( oid == null )
            {
                oid = getUUIDFactory().createUUID();
                schemaDescriptor.setUUID(oid);
            }
            uuid = oid.toString();

            aid = schemaDescriptor.getAuthorizationId();

            dbid = schemaDescriptor.getDatabaseId().toString();

        }

        /* Build the row to insert */
        row = getExecutionFactory().getValueRow(SYSSCHEMAS_COLUMN_COUNT);
        setRowColumns(row, name, uuid, aid, dbid);

        return row;
    }

    public static void setRowColumns(ExecRow row, String name, String uuid, String aid, String dbid) {
        /* 1st column is SCHEMAID */
        row.setColumn(1, new SQLChar(uuid));

        /* 2nd column is SCHEMANAME */
        row.setColumn(2, new SQLVarchar(name));

        /* 3rd column is SCHEMAAID */
        row.setColumn(3, new SQLVarchar(aid));

        /* 4th column is DATABASEID*/
        row.setColumn(4, new SQLChar(dbid));
    }


    ///////////////////////////////////////////////////////////////////////////
    //
    //    ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
    //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Make an  Tuple Descriptor out of a SYSSCHEMAS row
     *
     * @param row                     a SYSSCHEMAS row
     * @param parentTupleDescriptor    unused
     * @param dd                     dataDictionary
     *
     * @param tc
     * @return    a  descriptor equivalent to a SYSSCHEMAS row
     *
     * @exception   StandardException thrown on failure
     */
    public TupleDescriptor buildDescriptor(
            ExecRow row,
            TupleDescriptor parentTupleDescriptor,
            DataDictionary dd, TransactionController tc)
                    throws StandardException
    {
        DataValueDescriptor     col;
        SchemaDescriptor        descriptor;
        String                  name;
        UUID                    id;
        String                  aid;
        String                  uuid;
        UUID                    dbid;
        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(row.nColumns() == SYSSCHEMAS_COLUMN_COUNT,
                                 "Wrong number of columns for a SYSSCHEMAS row");
        }

        // first column is schemaid (UUID - char(36))
        col = row.getColumn(1);
        uuid = col.getString();
        id = getUUIDFactory().recreateUUID(uuid);

        // second column is schemaname (varchar(128))
        col = row.getColumn(2);
        name = col.getString();

        // third column is auid (varchar(128))
        col = row.getColumn(3);
        aid = col.getString();

        // fourth column is databaseid (char(36))
        col = row.getColumn(4);
        dbid = getUUIDFactory().recreateUUID(col.getString());

        descriptor = ddg.newSchemaDescriptor(name, aid, id, dbid);

        return descriptor;
    }

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[]    buildColumnList()
        throws StandardException
    {
            return new SystemColumn[] {
                SystemColumnImpl.getUUIDColumn("SCHEMAID", false),
                SystemColumnImpl.getIdentifierColumn("SCHEMANAME", false),
                SystemColumnImpl.getIdentifierColumn("AUTHORIZATIONID", false),
                SystemColumnImpl.getUUIDColumn("DATABASEID", false)
            };
    }

    public List<ColumnDescriptor[]> getViewColumns(TableDescriptor view, UUID viewId)
            throws StandardException
    {
        List<ColumnDescriptor[]> cdsl = new ArrayList<>();
        cdsl.add(
            new ColumnDescriptor[]{
                new ColumnDescriptor("SCHEMAID", 1, 1,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR,  false,  36),
                        null, null, view, viewId, 0, 0, 0),
                new ColumnDescriptor("SCHEMANAME", 2, 2,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,  false,  128),
                        null, null, view, viewId, 0, 0, 0),
                new ColumnDescriptor("AUTHORIZATIONID", 3, 3,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,  false,  128),
                        null, null, view, viewId, 0, 0, 0),
                //new ColumnDescriptor("DATABASEID", 4, 4,
                //        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, false, 36),
                //        null, null, view, viewId, 0, 0, 0)
        });
        return cdsl;
    }

    public static final String FILTER_SYS_SCHEMAS_OR_CURRENT_DB_SCHEMAS =
        " (s.schemaname in ('SYS', 'SYSIBM', 'SYSIBMADM', 'SYSC_UTIL', 'SYSFUN', 'SYSVW') OR " +
        " d.databasename = current server) ";

    private static final String REGULAR_USER_SCHEMA =
            "SELECT S.SCHEMAID, S.SCHEMANAME, S.AUTHORIZATIONID " +
            "FROM SYS.SYSSCHEMAS as S, SYS.SYSSCHEMAPERMS as P, SYS.SYSDATABASES as D " +
            "WHERE  S.schemaid = P.schemaid and P.accessPriv = 'y' and s.databaseid = d.databaseid and " +
                FILTER_SYS_SCHEMAS_OR_CURRENT_DB_SCHEMAS +
                "and P.grantee in (select name from sysvw.sysallroles) \n" +
            "UNION ALL " +
            "SELECT S.SCHEMAID, S.SCHEMANAME, S.AUTHORIZATIONID " +
            "FROM SYS.SYSSCHEMAS as S INNER JOIN SYS.SYSDATABASES as D ON s.databaseid = d.databaseid " +
            "WHERE " + FILTER_SYS_SCHEMAS_OR_CURRENT_DB_SCHEMAS + " AND " +
                "S.authorizationId in (select name from new com.splicemachine.derby.vti.SpliceGroupUserVTI(1) as b (NAME VARCHAR(128)))";

    private static final String SUPER_USER_SCHEMA =
            "SELECT S.SCHEMAID, S.SCHEMANAME, S.AUTHORIZATIONID " +
                    "FROM SYS.SYSSCHEMAS as S INNER JOIN SYS.SYSDATABASES as D on s.databaseid = d.databaseid ";

    private static final String PUBLIC_SCHEMA =
            "SELECT S.SCHEMAID, S.SCHEMANAME, S.AUTHORIZATIONID " +
                    "FROM SYS.SYSSCHEMAS as S where S.SCHEMANAME in ('SYSVW', 'SYSCS_UTIL', 'SYSFUN') ";

    public static final String SYSSCHEMASVIEW_VIEW_SQL = "create view sysschemasView as \n" +
            SUPER_USER_SCHEMA +
            "WHERE " + FILTER_SYS_SCHEMAS_OR_CURRENT_DB_SCHEMAS + " AND " +
                "CURRENT DATABASE ADMIN = (select name from new com.splicemachine.derby.vti.SpliceGroupUserVTI(2) as b (NAME VARCHAR(128))) \n" +
            "UNION ALL " +
            REGULAR_USER_SCHEMA +
            "UNION " +
            PUBLIC_SCHEMA;

    public static final String SYSSCHEMASVIEW_VIEW_SQL1 = "create view sysschemasView as \n" +
            SUPER_USER_SCHEMA + "WHERE " + FILTER_SYS_SCHEMAS_OR_CURRENT_DB_SCHEMAS;


    public static final String RANGER_USER_SCHEMA =
            "select S.SCHEMAID, S.SCHEMANAME, S.AUTHORIZATIONID from SYS.SYSSCHEMAS as S where S.SCHEMANAME in " +
            "(select name from new com.splicemachine.derby.vti.SchemaFilterVTI() as b (NAME VARCHAR(128))) ";


    public static final String SYSSCHEMASVIEW_VIEW_RANGER = "create view sysschemasView as \n" +
            RANGER_USER_SCHEMA;

}
