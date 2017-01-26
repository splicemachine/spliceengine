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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.iapi.types.SQLVarchar;
import java.sql.Types;

/**
 * Created by jyuan on 2/6/15.
 */
public class SYSBACKUPFILESETRowFactory extends CatalogRowFactory {

    private static final String TABLENAME_STRING = "SYSBACKUPFILESET";
    private static final int BACKUPFILESET_COLUMN_COUNT = 4;

    private static final int BACKUP_ITEM = 1;
    private static final int REGION_NAME = 2;
    private static final int FILE_NAME = 3;
    private static final int INCLUDE = 4;

    protected static final int SYSBACKUPFILESET_INDEX1_ID = 0;

    private	static	final	boolean[]	uniqueness = {
            true
    };

    private static final int[][] indexColumnPositions = {
            {BACKUP_ITEM, REGION_NAME,FILE_NAME}
    };


    private static String uuids[] = {
            "cb4d2219-fd7a-4003-9e42-beff555a365c",
            "cb4d2219-fd7c-4003-9e42-beff555a365c",
            "cb4d2219-fd7z-4003-9e42-beff555a365c"
    };

    public SYSBACKUPFILESETRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(BACKUPFILESET_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, uniqueness, uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        String backup_item = null;
        String region_name = null;
        String file_name = null;
        boolean include = false;

        if (td != null) {
            BackupFileSetDescriptor d = (BackupFileSetDescriptor)td;
            backup_item = d.getBackupItem();
            region_name = d.getRegionName();
            file_name = d.getFileName();
            include = d.shouldInclude();
        }

        ExecRow row = getExecutionFactory().getValueRow(BACKUPFILESET_COLUMN_COUNT);

        row.setColumn(BACKUP_ITEM, new SQLVarchar(backup_item));
        row.setColumn(REGION_NAME, new SQLVarchar(region_name));
        row.setColumn(FILE_NAME, new SQLVarchar(file_name));
        row.setColumn(INCLUDE, new SQLBoolean(include));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                    row.nColumns() == BACKUPFILESET_COLUMN_COUNT,
                    "Wrong number of columns for a BACKUP_FILESET row");
        }

        DataValueDescriptor col = row.getColumn(BACKUP_ITEM);
        String backupItem = col.getString();

        col = row.getColumn(REGION_NAME);
        String regionName = col.getString();

        col = row.getColumn(FILE_NAME);
        String fileName = col.getString();

        col = row.cloneColumn(INCLUDE);
        boolean include = col.getBoolean();

        return new BackupFileSetDescriptor(backupItem, regionName, fileName, include);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("BACKUP_ITEM", Types.VARCHAR, false, 32642),
                SystemColumnImpl.getColumn("REGION_NAME",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("FILE_NAME",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("INCLUDE",Types.BOOLEAN,false)
        };
    }
}
