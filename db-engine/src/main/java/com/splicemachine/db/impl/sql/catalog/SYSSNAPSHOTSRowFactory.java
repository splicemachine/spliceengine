/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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

public class SYSSNAPSHOTSRowFactory extends CatalogRowFactory {
    public static final String		TABLENAME_STRING = "SYSSNAPSHOTS";

    protected static final int		COLUMN_COUNT = 6;
    /* Column #s for sysSnapshots (1 based) */
    protected static final int		SNAPSHOTID = 1;
    protected static final int		SCOPE = 2;
    protected static final int		SCOPENAME = 3;
    protected static final int		STATUS = 4;
    protected static final int		BEGINTIMESTAMP =5;
    protected static final int		ENDTIMESTAMP =6;

    private	static	final	String[]	uuids = {
            "59cccae0-9272-11eb-a8b3-0242ac130003", // catalog UUID
            "59cccd2e-9272-11eb-a8b3-0242ac130003", // heap UUID
            "59ccce1e-9272-11eb-a8b3-0242ac130003"  // SYSTABLESNAPSHOTS_INDEX0
    };

    protected static final int SYSSNAPSHOTS_INDEX1_ID = 0;

    private	static	final	boolean[]	uniqueness = {
            true
    };

    private static final int[][] indexColumnPositions = {
            {SNAPSHOTID},
    };

    /////////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTORS
    //
    /////////////////////////////////////////////////////////////////////////////

    public SYSSNAPSHOTSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf, DataDictionary dd)
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
        String scope = null;
        String scopeName = null;
        String status = null;
        DateTime beginTimeStamp = null;
        DateTime endTimeStamp = null;

        if (td != null)
        {
            if (!(td instanceof SnapshotDescriptor))
                throw new RuntimeException("Unexpected TupleDescriptor " + td.getClass().getName());

            SnapshotDescriptor descriptor = (SnapshotDescriptor)td;
            snapshotId = descriptor.getSnapshotId();
            scope = descriptor.getScope();
            scopeName = descriptor.getScopeName();
            status = descriptor.getStatus();
            beginTimeStamp = descriptor.getBeginTimestamp();
            endTimeStamp = descriptor.getEndTimestamp();
        }

        ExecRow row = getExecutionFactory().getValueRow(COLUMN_COUNT);
        row.setColumn(SNAPSHOTID, new SQLLongint(snapshotId));
        row.setColumn(SCOPE, new SQLVarchar(scope));
        row.setColumn(SCOPENAME, new SQLVarchar(scopeName));
        row.setColumn(STATUS, new SQLVarchar(status));
        row.setColumn(BEGINTIMESTAMP, new SQLTimestamp(beginTimeStamp));
        row.setColumn(ENDTIMESTAMP, new SQLTimestamp(endTimeStamp));
        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary, TransactionController tc) throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                    row.nColumns() == COLUMN_COUNT,
                    "Wrong number of columns for a SYSBACKUP row");
        }
        DataValueDescriptor col = row.getColumn(SNAPSHOTID);
        long snapshotId = col.getLong();

        col = row.getColumn(SCOPE);
        String scope = col.getString();

        col = row.getColumn(SCOPENAME);
        String scopeName = col.getString();

        col = row.getColumn(STATUS);
        String status = col.getString();

        col = row.getColumn(BEGINTIMESTAMP);
        DateTime beginTimestamp = col.getDateTime();

        col = row.getColumn(ENDTIMESTAMP);
        DateTime endTimestamp = col.getDateTime();

        return new SnapshotDescriptor(snapshotId, scope, scopeName, status, beginTimestamp, endTimestamp);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException
    {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("SNAPSHOTID", Types.BIGINT,false),
                SystemColumnImpl.getColumn("SCOPE",Types.VARCHAR,false,128),
                SystemColumnImpl.getColumn("SCOPENAME",Types.VARCHAR,false,128),
                SystemColumnImpl.getColumn("STATUS", Types.VARCHAR,false,128),
                SystemColumnImpl.getColumn("BEGINTIMESTAMP",Types.TIMESTAMP,true),
                SystemColumnImpl.getColumn("ENDTIMESTAMP",Types.TIMESTAMP,true),
        };
    }
}
