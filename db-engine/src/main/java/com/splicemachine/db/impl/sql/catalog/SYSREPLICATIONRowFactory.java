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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.db.iapi.types.SQLVarchar;
import org.joda.time.DateTime;

import java.sql.Types;

/**
 * Created by jyuan on 12/11/19.
 */
public class SYSREPLICATIONRowFactory extends CatalogRowFactory {

    public static final String SCOPE_DATABASE = "DATABASE";
    public static final String SCOPE_SCHEMA = "SCHEMA";
    public static final String SCOPE_TABLE = "TABLE";

    private static final String TABLENAME_STRING = "SYSREPLICATION";
    private static final int REPLICATION_COLUMN_COUNT = 3;

    private static final int SCOPE = 1;
    private static final int SCHEMA_NAME = 2;
    private static final int TABLE_NAME = 3;


    protected static final int SYSREPLICATION_INDEX1_ID = 0;
    protected static final int SYSREPLICATION_INDEX2_ID = 1;
    protected static final int SYSREPLICATION_INDEX3_ID = 2;

    private static String uuids[] = {
            "1e9eb25e-1c61-11ea-978f-2e728ce88125", // catalog UUID
            "28411450-1c61-11ea-978f-2e728ce88125", // heap UUID
            "f54e794c-1c6b-11ea-978f-2e728ce88125", // index1 UUID
            "818b8290-1d0e-11ea-978f-2e728ce88125", // index2 UUID
            "8bd68c7e-1d2f-11ea-978f-2e728ce88125"  // index3 UUID
    };

    private	static	final	boolean[]	uniqueness = {
            false,
            false,
            true
    };

    private static final int[][] indexColumnPositions =
            {
                    {SCOPE},
                    {SCOPE, SCHEMA_NAME},
                    {SCOPE, SCHEMA_NAME, TABLE_NAME}
            };

    public SYSREPLICATIONRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf, DataDictionary dd) {
        super(uuidf, ef, dvf, dd);
        initInfo(REPLICATION_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, uniqueness, uuids);
    }

    @Override
    public ExecRow makeRow(boolean latestVersion, TupleDescriptor td, TupleDescriptor parent) throws StandardException {
        String scope = null;
        String schemaName = null;
        String tableName = null;
        if (td != null) {
            if (!(td instanceof ReplicationDescriptor))
                throw new RuntimeException("Unexpected TupleDescriptor " + td.getClass().getName());

            ReplicationDescriptor d = (ReplicationDescriptor)td;
            scope = d.getScope();
            schemaName = d.getSchemaName();
            tableName = d.getTableName();
        }

        ExecRow row = getExecutionFactory().getValueRow(REPLICATION_COLUMN_COUNT);
        row.setColumn(SCOPE, new SQLVarchar(scope));
        row.setColumn(SCHEMA_NAME, new SQLVarchar(schemaName));
        row.setColumn(TABLE_NAME, new SQLVarchar(tableName));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException {

        DataValueDescriptor col = row.getColumn(SCOPE);
        String scope = col.getString();

        col = row.getColumn(SCHEMA_NAME);
        String schemaName = col.getString();

        col = row.getColumn(TABLE_NAME);
        String tableName = col.getString();


        return new ReplicationDescriptor(scope, schemaName, tableName);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("SCOPE", Types.VARCHAR,false,10),
                SystemColumnImpl.getColumn("SCHEMANAME", Types.VARCHAR,true,128),
                SystemColumnImpl.getColumn("TABLENAME", Types.VARCHAR,true,128)
        };
    }
}
