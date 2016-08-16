/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.Iterator;

/**
 *
 */
@NotThreadSafe
public class ResultSetIterator implements Iterable<LocatedRow>, Iterator<LocatedRow>, Closeable {
    private ExecRow execRow;
    private ResultSet resultSet;
    private PreparedStatement ps;
    private boolean[] isNullable;
    private Connection connection;


    public ResultSetIterator(Connection connection, PreparedStatement ps, ExecRow execRow) {
        this.connection = connection;
        this.ps = ps;
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
    public Iterator<LocatedRow> iterator() {
        return this;
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
    public LocatedRow next() {
        try {
            ExecRow returnRow = execRow.getClone();
            for (int i = 1; i <= execRow.nColumns(); i++) {
                returnRow.getColumn(i).setValueFromResultSet(resultSet,i,isNullable[i-1]);
            }
            return new LocatedRow(returnRow);
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
        SQLException error = null;
        if(resultSet!=null){
            try{
                resultSet.close();
            }catch(SQLException se){
               error = se;
            }
        }
        if(ps!=null){
            try{
                ps.close();
            }catch(SQLException se){
                if(error==null) error = se;
                else
                    error.setNextException(se);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException se) {
                if(error==null) error = se;
                else error.setNextException(se);
            }
        }
        if(error!=null)
            throw new IOException(error);
    }
}
