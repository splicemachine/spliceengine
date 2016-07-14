/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;
import org.joda.time.DateTime;

import java.sql.Types;

/**
 * Created by jyuan on 3/24/15.
 */
public class SYSBACKUPJOBSRowFactory extends CatalogRowFactory {
    private static final String TABLENAME_STRING = "SYSBACKUPJOBS";
    private static final int BACKUPJOBS_COLUMN_COUNT = 5;

    private static final int JOB_ID = 1;
    private static final int FILESYSTEM = 2;
    private static final int TYPE = 3;
    private static final int HOUR = 4;
    private static final int BEGIN_TIMESTAMP = 5;

    private static String uuids[] = {
            "691df6e2-de85-4311-827b-8e00e38d7aab",
            "691df6e2-de8x-4311-827b-8e00e38d7aab",
            "691df6e2-de8y-4311-827b-8e00e38d7aab"

    };


    protected static final int SYSBACKUPJOBS_INDEX1_ID = 0;
    protected static final int SYSBACKUPJOBS_INDEX2_ID = 1;

    private	static	final	boolean[]	uniqueness = {
            true
    };

    private static final int[][] indexColumnPositions = {
                    {JOB_ID}
            };


    public SYSBACKUPJOBSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(BACKUPJOBS_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, uniqueness, uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        long jobId = 0;
        String fileSystem = null;
        String type = null;
        int hourOfDay = 0;
        DateTime beginTimestamp = null;

        if (td != null) {
            BackupJobsDescriptor d = (BackupJobsDescriptor)td;
            jobId = d.getJobId();
            fileSystem = d.getFileSystem();
            type = d.getType();
            hourOfDay = d.getHourOfDay();
            beginTimestamp = d.getBeginTimestamp();
        }

        ExecRow row = getExecutionFactory().getValueRow(BACKUPJOBS_COLUMN_COUNT);

        row.setColumn(JOB_ID, new SQLLongint(jobId));
        row.setColumn(FILESYSTEM, new SQLVarchar(fileSystem));
        row.setColumn(TYPE, new SQLVarchar(type));
        row.setColumn(HOUR, new SQLInteger(hourOfDay));
        row.setColumn(BEGIN_TIMESTAMP, new SQLTimestamp(beginTimestamp));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                    row.nColumns() == BACKUPJOBS_COLUMN_COUNT,
                    "Wrong number of columns for a BACKUP_FILESET row");
        }

        DataValueDescriptor col = row.getColumn(JOB_ID);
        long jobId = col.getLong();

        col = row.getColumn(FILESYSTEM);
        String fileSystem = col.getString();

        col = row.getColumn(TYPE);
        String type = col.getString();

        col = row.cloneColumn(HOUR);
        int hour = col.getInt();

        col = row.cloneColumn(BEGIN_TIMESTAMP);
        DateTime beginTimestamp = col.getDateTime();

        return new BackupJobsDescriptor(jobId, fileSystem, type, hour, beginTimestamp);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("JOB_ID", Types.BIGINT, false),
                SystemColumnImpl.getColumn("FILESYSTEM",Types.VARCHAR,false, 4000),
                SystemColumnImpl.getColumn("TYPE",Types.VARCHAR,false,32),
                SystemColumnImpl.getColumn("HOUR_OF_DAY",Types.INTEGER,false),
                SystemColumnImpl.getColumn("BEGIN_TIMESTAMP",Types.TIMESTAMP,false)
        };
    }
}
