/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
 *
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
import com.splicemachine.db.iapi.types.SQLBit;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.db.iapi.types.SQLVarchar;
import org.joda.time.DateTime;

import java.sql.Types;

/**
 * Created by jyuan on 6/6/17.
 */
public class SYSTOKENSRowFactory extends CatalogRowFactory
{
    public static final String		TABLENAME_STRING = "SYSTOKENS";
    public static final String      SYSTOKENS_UUID = "39ac6233-8831-4262-aa7a-84acce449ad1";

    protected static final int		COLUMN_COUNT = 5;

    /* Column #s for systablesnapshots (1 based) */
    protected static final int		TOKEN = 1;
    protected static final int		USERNAME = 2;
    protected static final int		CREATIONTIME = 3;
    protected static final int		EXPIRETIME = 4;
    protected static final int		MAXIMUMTIME = 5;

    private	static	final	String[]	uuids =
            {
                     SYSTOKENS_UUID                         // catalog UUID
                    ,"122cf322-e914-4f40-b619-e2f8d78c0381"	// heap UUID
                    ,"c34db556-b35b-4bd1-ad4c-0eaa106c91f3"	// SYSTABLESNAPSHOTS_INDEX1
            };

    protected static final int SYSTOKENS_INDEX1_ID = 0;

    private	static	final	boolean[]	uniqueness = {
            true
    };

    private static final int[][] indexColumnPositions =
            {
                    {TOKEN},
            };

    /////////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTORS
    //
    /////////////////////////////////////////////////////////////////////////////

    public SYSTOKENSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf, DataDictionary dd)
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
     * Make a SYSTABLESNAPSHOTS row
     *
     * @return	Row suitable for inserting into SYSTABLES.
     *
     * @exception StandardException thrown on failure
     */

    public ExecRow makeRow(boolean latestVersion, TupleDescriptor td, TupleDescriptor parent)
            throws StandardException
    {

        byte[] token = null;
        String userName = null;
        DateTime creationTime = null;
        DateTime expireTime = null;
        DateTime maxTime = null;

        if (td != null)
        {
            if (!(td instanceof TokenDescriptor))
                throw new RuntimeException("Unexpected TupleDescriptor " + td.getClass().getName());

            TokenDescriptor descriptor = (TokenDescriptor)td;
            token = descriptor.getToken();
            userName = descriptor.getUserName();
            creationTime = descriptor.getCreationTime();
            expireTime = descriptor.getExpireTime();
            maxTime = descriptor.getMaxTime();
        }

        ExecRow row = getExecutionFactory().getValueRow(COLUMN_COUNT);
        row.setColumn(TOKEN, new SQLBit(token));
        row.setColumn(USERNAME, new SQLVarchar(userName));
        row.setColumn(CREATIONTIME, new SQLTimestamp(creationTime));
        row.setColumn(EXPIRETIME, new SQLTimestamp(expireTime));
        row.setColumn(MAXIMUMTIME, new SQLTimestamp(maxTime));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                    row.nColumns() == COLUMN_COUNT,
                    "Wrong number of columns for a SYSBACKUP row");
        }
        DataValueDescriptor col = row.getColumn(TOKEN);
        byte[] token = col.getBytes();

        col = row.getColumn(USERNAME);
        String userName = col.getString();

        col = row.getColumn(CREATIONTIME);
        DateTime creationTime = col.getDateTime();

        col = row.getColumn(EXPIRETIME);
        DateTime expireTime = col.getDateTime();

        col = row.getColumn(MAXIMUMTIME);
        DateTime maxTime = col.getDateTime();

        return new TokenDescriptor(token, userName, creationTime, expireTime, maxTime);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException
    {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("TOKEN",Types.BINARY,false),
                SystemColumnImpl.getColumn("USERNAME",Types.VARCHAR,false,128),
                SystemColumnImpl.getColumn("CREATIONTIME",Types.TIMESTAMP,false),
                SystemColumnImpl.getColumn("EXPIRETIME",Types.TIMESTAMP,false),
                SystemColumnImpl.getColumn("MAXIMUMTIME",Types.TIMESTAMP,false)
        };
    }
}
