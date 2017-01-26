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

/**
 * Created by jyuan on 2/6/15.
 */

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;
import org.joda.time.DateTime;

import java.sql.Types;

public class SYSBACKUPRowFactory extends CatalogRowFactory {
    private static final String TABLENAME_STRING = "SYSBACKUP";
    private static final int BACKUP_COLUMN_COUNT = 9;

    private static final int BACKUP_ID = 1;
    private static final int BEGIN_TIMESTAMP = 2;
    private static final int END_TIMESTAMP = 3;
    private static final int STATUS = 4;
    private static final int FILESYSTEM = 5;
    private static final int SCOPE = 6;
    private static final int IS_INCREMENTAL_BACKUP = 7;
    private static final int INCREMENTAL_PARENT_BACKUP_ID = 8;
    private static final int ITEMS = 9;


    protected static final int SYSBACKUP_INDEX1_ID = 0;

    private	static	final	boolean[]	uniqueness = {
            true,
            false
    };

    private static final int[][] indexColumnPositions =
            {
                    {BACKUP_ID},
            };




    private static String uuids[] = {
            "6e205c88-4c1b-464a-b0ab-865d64b3279d",
            "6e205c88-4c1c-464a-b0ab-865d64b3279d",
            "6e205c88-4c1d-464a-b0ab-865d64b3279d",
            "6e205c88-4c1e-464a-b0ab-865d64b3279d"
    };

    public SYSBACKUPRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(BACKUP_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, uniqueness, uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        long backupId = 0;
        DateTime beginTimestamp = null;
        DateTime endTimestamp = null;
        String status = null;
        String fileSystem = null;
        String scope = null;
        boolean isIncremental = false;
        long parentId = -1;
        int items = 0;

        if (td != null) {
            BackupDescriptor d = (BackupDescriptor)td;
            backupId = d.getBackupId();
            beginTimestamp = d.getBeginTimestamp();
            endTimestamp = d.getEndTimestamp();
            status = d.getStatus();
            fileSystem = d.getFileSystem();
            scope = d.getScope();
            isIncremental = d.isIncremental();
            parentId = d.getParentBackupId();
            items = d.getItems();
        }

        ExecRow row = getExecutionFactory().getValueRow(BACKUP_COLUMN_COUNT);

        row.setColumn(BACKUP_ID, new SQLLongint(backupId));
        row.setColumn(BEGIN_TIMESTAMP, new SQLTimestamp(beginTimestamp));
        row.setColumn(END_TIMESTAMP, new SQLTimestamp(endTimestamp));
        row.setColumn(STATUS, new SQLVarchar(status));
        row.setColumn(FILESYSTEM, new SQLVarchar(fileSystem));
        row.setColumn(SCOPE, new SQLVarchar(scope));
        row.setColumn(IS_INCREMENTAL_BACKUP, new SQLBoolean(isIncremental));
        row.setColumn(INCREMENTAL_PARENT_BACKUP_ID, new SQLLongint(parentId));
        row.setColumn(ITEMS, new SQLInteger(items));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                    row.nColumns() == BACKUP_COLUMN_COUNT,
                    "Wrong number of columns for a SYSBACKUP row");
        }

        DataValueDescriptor col = row.getColumn(BACKUP_ID);
        long backupId = col.getLong();

        col = row.getColumn(BEGIN_TIMESTAMP);
        DateTime beginTimestamp = col.getDateTime();

        col = row.getColumn(END_TIMESTAMP);
        DateTime endTimestamp = col.getDateTime();

        col = row.getColumn(STATUS);
        String status = col.getString();

        col = row.getColumn(FILESYSTEM);
        String fileSystem = col.getString();

        col = row.getColumn(SCOPE);
        String scope = col.getString();

        col = row.getColumn(IS_INCREMENTAL_BACKUP);
        boolean isIncremental = col.getBoolean();

        col = row.getColumn(INCREMENTAL_PARENT_BACKUP_ID);
        long parentId = col.getLong();

        col = row.getColumn(ITEMS);
        int items = col.getInt();

        return new BackupDescriptor(backupId, beginTimestamp, endTimestamp, status, fileSystem,
                scope, isIncremental, parentId, items);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("BACKUP_ID", Types.BIGINT, false),
                SystemColumnImpl.getColumn("BEGIN_TIMESTAMP",Types.TIMESTAMP,false),
                SystemColumnImpl.getColumn("END_TIMESTAMP",Types.TIMESTAMP,false),
                SystemColumnImpl.getColumn("STATUS",Types.VARCHAR,false,20),
                SystemColumnImpl.getColumn("FILESYSTEM",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("SCOPE",Types.VARCHAR,false,10),
                SystemColumnImpl.getColumn("INCREMENTAL_BACKUP",Types.BOOLEAN,false),
                SystemColumnImpl.getColumn("INCREMENTAL_PARENT_BACKUP_ID",Types.BIGINT,true),
                SystemColumnImpl.getColumn("BACKUP_ITEM",Types.INTEGER, true),
        };
    }
}