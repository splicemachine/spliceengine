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

package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.function.IteratorUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Iterator;

/**
 *
 */
@NotThreadSafe
public class ResultSetIterator implements Iterable<ExecRow>, Iterator<ExecRow>, Closeable {
    private ExecRow execRow;
    private ResultSet resultSet;
    private boolean[] isNullable;
    private Connection connection;


    public ResultSetIterator(Connection connection, PreparedStatement ps, ExecRow execRow) {
        this.connection = connection;
        try {
            this.resultSet = ps.executeQuery();
            ResultSetMetaData rsm = resultSet.getMetaData();
            isNullable = new boolean[rsm.getColumnCount()];
            for (int i =0;i<rsm.getColumnCount();i++) {
                isNullable[i] =rsm.isNullable(i+1)!=0;
            }
        }  catch (Exception e) {
            try {
                if (connection != null)
                    connection.close();
            } catch (Exception cE) {
                throw new RuntimeException(cE);
            }
            throw new RuntimeException(e);
        }
        this.execRow = execRow;
    }

    @Override
    public Iterator<ExecRow> iterator() {
        return IteratorUtils.asInterruptibleIterator(this);
    }

    @Override
    public boolean hasNext() {
        try {
            return resultSet.next();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecRow next() {
        try {
            ExecRow returnRow = execRow.getClone();
            for (int i = 1; i <= execRow.nColumns(); i++) {
                returnRow.getColumn(i).setValueFromResultSet(resultSet,i,isNullable[i-1]);
            }
            return returnRow;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception se) {
                throw new IOException(se);
            }
        }
    }
}
