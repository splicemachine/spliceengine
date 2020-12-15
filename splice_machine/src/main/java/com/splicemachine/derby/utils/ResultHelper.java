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
 */

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.joda.time.DateTime;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * A helper to create a ResultSet with columns
 * use e.g.
 *   ResultHelper resultHelper = new ResultHelper();
 *
 *   // order here determines order in column
 *   ResultHelper.VarcharColumn   colUser    = resultHelper.addVarchar("USER", 10);
 *   ResultHelper.TimestampColumn colTime    = resultHelper.addTimestamp("TIME", 30);
 *   ResultHelper.BigintColumn    colNumber  = resultHelper.addBigint("NUMBER", 10);
 *
 *   for(...)
 *   {
 *       resultHelper.newRow();
 *       colTime.set( new DateTime(...) );
 *       colUser.set( userName );
 *       if( hasNumber ) // if not set, will be displayed as NULL
 *          colNumber.set( 23 );
 *   }
 *   ResultSet rs = resultHelper.getResultSet();
 *   resultSet[0] = rs;
 *
 */
class ResultHelper {
    public VarcharColumn addVarchar(String name, int length) {
        VarcharColumn c = new VarcharColumn();
        c.add(name, length);
        return c;
    }

    public BigintColumn addBigint(String name, int length)
    {
        BigintColumn c = new BigintColumn();
        c.add(name, length);
        return c;
    }

    public TimestampColumn addTimestamp(String name, int length)
    {
        TimestampColumn c = new TimestampColumn();
        c.add(name, length);
        return c;
    }

    public int numColumns() {
        return columnDescriptors.size();
    }

    public ResultColumnDescriptor[] getColumnDescriptorsArray() {
        return columnDescriptors.toArray(new ResultColumnDescriptor[columnDescriptors.size()]);
    }

    void newRow()
    {
        finishRow();
        row = new ValueRow(numColumns());
    }
    void finishRow() {
        if( row == null ) return;
        for(Column c : columns) c.finishRow();
        rows.add(row);
        row = null;
    }

    public ResultSet getResultSet() throws SQLException {
        finishRow();

        EmbedConnection conn = (EmbedConnection) BaseAdminProcedures.getDefaultConn();
        Activation lastActivation = conn.getLanguageConnection().getLastActivation();

        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, getColumnDescriptorsArray(), lastActivation);
        try {
            resultsToWrap.openCore();
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
        return new EmbedResultSet40(conn, resultsToWrap, false, null, true);
    }

    class Column {
        public int index;
        boolean set = false;

        public void finishRow() {
            if( !set ) init();
            set = false;
        }

        void add(String name, int length)
        {
            columnDescriptors.add( new GenericColumnDescriptor(name, create(length)) );
            index = columnDescriptors.size();
            columns.add(this);
        }

        void init() { throw new RuntimeException("not implemented"); }

        public DataTypeDescriptor create(int length) { throw new RuntimeException("not implemented"); }
    }

    class VarcharColumn extends Column {
        @Override
        public DataTypeDescriptor create(int length)
        {
            return DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, length);
        }
        public void set(String value) {
            assert row != null;
            row.setColumn(index, new SQLVarchar(value));
            set = true;
        }
        @Override
        public void init() {
            set("");
        }
    }
    class BigintColumn extends Column {
        @Override
        public DataTypeDescriptor create(int length)
        {
            return DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT, length);
        }
        public void set(long value) {
            assert row != null;
            row.setColumn(index, new SQLLongint(value));
            set = true;
        }
        @Override
        public void init() {
            set(0);
        }
    }

    class TimestampColumn extends Column {
        @Override
        public DataTypeDescriptor create(int length)
        {
            return DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP, length);
        }
        public void set(org.joda.time.DateTime value) throws StandardException {
            assert row != null;
            row.setColumn(index, new SQLTimestamp(value));
            set = true;
        }
        @Override
        public void init() {
            try {
                set( new DateTime(0));
            } catch (StandardException e) {
            }
        }
    }
    private List<ExecRow> rows = new ArrayList<>();
    private List<Column> columns = new ArrayList<>();
    private List<GenericColumnDescriptor> columnDescriptors = new ArrayList<>();
    private ExecRow row;
}
