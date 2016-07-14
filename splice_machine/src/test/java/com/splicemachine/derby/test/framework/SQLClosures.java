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
