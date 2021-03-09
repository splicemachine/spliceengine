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
 * Factory for creating a SYSDATABASES row.
 *
 *
 * @version 0.1
 */

public class SYSDATABASESRowFactory extends CatalogRowFactory
{
    public static    final    String    TABLENAME_STRING = "SYSDATABASES";

    public static final int SYSDATABASES_COLUMN_COUNT = 3;
    /* Column #s for sysinfo (1 based) */
    public static final int SYSDATABASES_DATABASEID = 1;
    public static final int SYSDATABASES_DATABASENAME = 2;
    public static final int SYSDATABASES_DATABASEAID = 3;


    public static final int SYSDATABASES_INDEX1_ID = 0;
    public static final int SYSDATABASES_INDEX2_ID = 1;


    private static final int[][] indexColumnPositions =
    {
        {SYSDATABASES_DATABASENAME},
        {SYSDATABASES_DATABASEID}
    };

    private    static    final    boolean[]    uniqueness = null;

    private    static    final    String[]    uuids =
    {
         "80000062-00d0-fd77-3ed8-000a0a0b1900"    // catalog UUID
        ,"8000006a-00d0-fd77-3ed8-000a0a0b1900"    // heap UUID
        ,"80000064-00d0-fd77-3ed8-000a0a0b1900"    // SYSDATABASES_INDEX1
        ,"80000066-00d0-fd77-3ed8-000a0a0b1900"    // SYSDATABASES_INDEX2
    };

    /////////////////////////////////////////////////////////////////////////////
    //
    //    CONSTRUCTORS
    //
    /////////////////////////////////////////////////////////////////////////////

    public SYSDATABASESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf, DataDictionary dd)
    {
        super(uuidf,ef,dvf, dd);
        initInfo(SYSDATABASES_COLUMN_COUNT, TABLENAME_STRING,
                indexColumnPositions, uniqueness, uuids );
    }

    SYSDATABASESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
    {
        super(uuidf,ef,dvf);
        initInfo(SYSDATABASES_COLUMN_COUNT, TABLENAME_STRING,
                 indexColumnPositions, uniqueness, uuids );
    }

    /////////////////////////////////////////////////////////////////////////////
    //
    //    METHODS
    //
    /////////////////////////////////////////////////////////////////////////////

  /**
     * Make a SYSDATABASES row
     *
     * @return    Row suitable for inserting into SYSDATABASES.
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
            if (!(td instanceof DatabaseDescriptor))
                throw new RuntimeException("Unexpected DatabaseDescriptor " + td.getClass().getName());

            DatabaseDescriptor    databaseDescriptor = (DatabaseDescriptor)td;

            name = databaseDescriptor.getDatabaseName();
            oid = databaseDescriptor.getUUID();
            if ( oid == null )
            {
                oid = getUUIDFactory().createUUID();
                databaseDescriptor.setUUID(oid);
            }
            uuid = oid.toString();

            aid = databaseDescriptor.getAuthorizationId();
        }

        /* Build the row to insert */
        row = getExecutionFactory().getValueRow(SYSDATABASES_COLUMN_COUNT);
        setRowColumns(row, name, uuid, aid);

        return row;
    }

    public static void setRowColumns(ExecRow row, String name, String uuid, String aid) {
        /* 1st column is DATABASEID */
        row.setColumn(1, new SQLChar(uuid));

        /* 2nd column is DATABASENAME */
        row.setColumn(2, new SQLVarchar(name));

        /* 3rd column is DATABASEAID */
        row.setColumn(3, new SQLVarchar(aid));
    }


    ///////////////////////////////////////////////////////////////////////////
    //
    //    ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
    //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Make an  Tuple Descriptor out of a SYSDATABASES row
     *
     * @param row                     a SYSDATABASES row
     * @param parentTupleDescriptor    unused
     * @param dd                     dataDictionary
     *
     * @param tc
     * @return    a  descriptor equivalent to a SYSDATABASES row
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
        DatabaseDescriptor        descriptor;
        String                  name;
        UUID                    id;
        String                  aid;
        String                  uuid;
        UUID                    dbid;
        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(row.nColumns() == SYSDATABASES_COLUMN_COUNT,
                                 "Wrong number of columns for a SYSDATABASES row");
        }

        // first column is databaseid (UUID - char(36))
        col = row.getColumn(1);
        uuid = col.getString();
        id = getUUIDFactory().recreateUUID(uuid);

        // second column is databasename (varchar(128))
        col = row.getColumn(2);
        name = col.getString();

        // third column is auid (varchar(128))
        col = row.getColumn(3);
        aid = col.getString();

        descriptor = ddg.newDatabaseDescriptor(name, aid, id);

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
                SystemColumnImpl.getUUIDColumn("DATABASEID", false),
                SystemColumnImpl.getIdentifierColumn("DATABASENAME", false),
                SystemColumnImpl.getIdentifierColumn("AUTHORIZATIONID", false)
            };
    }

    public List<ColumnDescriptor[]> getViewColumns(TableDescriptor view, UUID viewId)
            throws StandardException
    {
        List<ColumnDescriptor[]> cdsl = new ArrayList<>();
        cdsl.add(
            new ColumnDescriptor[]{
                new ColumnDescriptor("DATABASEID", 1, 1,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR,  false,  36),
                        null, null, view, viewId, 0, 0, 0),
                new ColumnDescriptor("DATABASENAME", 2, 2,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,  false,  128),
                        null, null, view, viewId, 0, 0, 0),
                new ColumnDescriptor("AUTHORIZATIONID", 3, 3,
                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,  false,  128),
                        null, null, view, viewId, 0, 0, 0)
        });
        return cdsl;
    }

    // XXX(arnaud multidb) create SYSDATABASEPERMS

    public static final String SYSDATABASES_VIEW_SQL = "create view sysdatabasesView as \n" +
            "SELECT D.* " +
                "FROM SYS.SYSDATABASES AS D ";
}
