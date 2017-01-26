/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.test.framework;

import java.sql.*;

/**
 * Utility class for performing SQL operations within convenient class
 * structures. Removes unneeded boilerplate code from unit tests
 *
 * @author Scott Fines
 * Date: 7/11/14
 */
public class SQLClosures {

    public interface SQLAction<T>{
        void execute(T resultSet) throws Exception;
    }

    public static void execute(Connection conn, SQLAction<Statement> sqlAction) throws Exception {
        Statement s = null;
        try{
            s = conn.createStatement();
            sqlAction.execute(s);
        }finally{
            if(s!=null)
                s.close();
        }
    }


    public static void query(Connection connection,String querySql,SQLAction<ResultSet> action) throws Exception {
        Statement s = null;
        ResultSet rs = null;
        try{
            s = connection.createStatement();
            rs = s.executeQuery(querySql);
            action.execute(rs);
        }finally{
            if(rs!=null)
                rs.close();
            if(s!=null)
                s.close();
        }
    }

    public static void prepareExecute(Connection connection,String sql,SQLAction<PreparedStatement> action) throws Exception {
        PreparedStatement ps = null;
        try{
            ps = connection.prepareStatement(sql);
            action.execute(ps);
        }finally{
            if(ps!=null)
                ps.close();
        }
    }

}
