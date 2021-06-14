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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.*;
import org.joda.time.DateTime;

import java.sql.Types;

public class SYSSNAPSHOTITEMSRowFactory extends CatalogRowFactory
{
    public static final String		TABLENAME_STRING = "SYSSNAPSHOTITEMS";

    protected static final int		COLUMN_COUNT = 6;

    /* Column #s for sysSnapshotItems (1 based) */
    protected static final int		SNAPSHOTID = 1;
    protected static final int		SNAPSHOTNAME = 2;
    protected static final int		SCHEMANAME = 3;
    protected static final int		OBJECTNAME = 4;
    protected static final int		CONGLOMERATENUMBER = 5;
    protected static final int		CREATIONTIME =6;

    private	static	final	String[]	uuids =
            {
                    "3292e23b-c605-4b0b-b8b7-adfed36a0096"	// catalog UUID
                    ,"744262c9-e1d0-4725-bff8-5823f0365ba1"	// heap UUID
                    ,"d0c86b7c-0d9f-424c-8c25-c3896b7a0f25"	// SYSTABLESNAPSHOTITEMS_INDEX1
            };

    protected static final int SYSSNAPSHOTITEMS_INDEX1_ID = 0;

    private	static	final	boolean[]	uniqueness = {
            false
    };

    private static final int[][] indexColumnPositions = {
            {SNAPSHOTID}
    };

    /////////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTORS
    //
    /////////////////////////////////////////////////////////////////////////////

    public SYSSNAPSHOTITEMSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf, DataDictionary dd)
    {
        super(uuidf,ef,dvf,dd);
        initInfo(COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, uniqueness, uuids);
    }

    /////////////////////////////////////////////////////////////////////////////
    //
    //	METHODS
    //
    /////////////////////////////////////////////////////////////////////////////

    /**
     * Make a SYSTABLESNAPSHOTITEMS row
     *
     * @return	Row suitable for inserting into SYSTABLES.
     *
     * @exception StandardException thrown on failure
     */

    public ExecRow makeRow(boolean latestVersion, TupleDescriptor td, TupleDescriptor parent)
            throws StandardException
    {

        long snapshotId = -1;
        String snapshotName = null;
        String schemaName = null;
        String objectName = null;
        long conglomerateNumber = 0;
        DateTime creationTime = null;

        if (td != null)
        {
            if (!(td instanceof SnapshotItemDescriptor))
                throw new RuntimeException("Unexpected TupleDescriptor " + td.getClass().getName());

            SnapshotItemDescriptor descriptor = (SnapshotItemDescriptor)td;
            snapshotId = descriptor.getSnapshotId();
            snapshotName = descriptor.getSnapshotName();
            schemaName = descriptor.getSchemaName();
            objectName = descriptor.getObjectName();
            conglomerateNumber = descriptor.getConglomerateNumber();
            creationTime = descriptor.getCreationTime();
        }

        ExecRow row = getExecutionFactory().getValueRow(COLUMN_COUNT);
        row.setColumn(SNAPSHOTID, new SQLLongint(snapshotId));
        row.setColumn(SNAPSHOTNAME, new SQLVarchar(snapshotName));
        row.setColumn(SCHEMANAME, new SQLVarchar(schemaName));
        row.setColumn(OBJECTNAME, new SQLVarchar(objectName));
        row.setColumn(CONGLOMERATENUMBER, new SQLLongint(conglomerateNumber));
        row.setColumn(CREATIONTIME, new SQLTimestamp(creationTime));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary,
                                           TransactionController tc) throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                    row.nColumns() == COLUMN_COUNT,
                    "Wrong number of columns for a SYSBACKUP row");
        }
        DataValueDescriptor col = row.getColumn(SNAPSHOTID);
        long snapshotId = col.getLong();

        col = row.getColumn(SNAPSHOTNAME);
        String snapshotName = col.getString();

        col = row.getColumn(SCHEMANAME);
        String schemaName = col.getString();

        col = row.getColumn(OBJECTNAME);
        String objectName = col.getString();

        col = row.getColumn(CONGLOMERATENUMBER);
        long conglomerateNumber = col.getLong();

        col = row.getColumn(CREATIONTIME);
        DateTime creationTime = col.getDateTime();

        return new SnapshotItemDescriptor(snapshotId, snapshotName, schemaName, objectName, conglomerateNumber, creationTime);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException
    {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("SNAPSHOTID",Types.BIGINT,false),
                SystemColumnImpl.getColumn("SNAPSHOTNAME",Types.VARCHAR,false,128),
                SystemColumnImpl.getColumn("SCHEMANAME",Types.VARCHAR,false,128),
                SystemColumnImpl.getColumn("OBJECTNAME",Types.VARCHAR,false,128),
                SystemColumnImpl.getColumn("CONGLOMERATENUMBER", Types.BIGINT, false),
                SystemColumnImpl.getColumn("CREATIONTIME",Types.TIMESTAMP,true),
        };
    }
}
