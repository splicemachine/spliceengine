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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author Scott Fines
 *         Date: 6/21/16
 */
public class TestConnectionPool{
    private ConnectionHolder currentConnection;

    public TestConnection getConnection() throws SQLException{
        return getConnection(SpliceNetConnection.DEFAULT_USER,SpliceNetConnection.DEFAULT_USER_PASSWORD);
    }

    public TestConnection getConnection(String user, String pwd) throws SQLException{
        String url = String.format(SpliceNetConnection.DB_URL_LOCAL,user,pwd);
        return getConnection(url);
    }

    public TestConnection getConnection(String url) throws SQLException{
        if(currentConnection!=null){
            if(currentConnection.isValid() && currentConnection.matches(url))
                return currentConnection.conn;

            currentConnection.conn.close();
        }
        currentConnection = new ConnectionHolder(url);
        return currentConnection.conn;
    }

    public void close() throws SQLException{
        if(currentConnection!=null){
            currentConnection.conn.close();
        }

    }

    private static class ConnectionHolder{
        final TestConnection conn;
        private final String url;

        ConnectionHolder(String url) throws SQLException{
            this.url = url;
            this.conn = new TestConnection(DriverManager.getConnection(url,new Properties()));
        }

        boolean matches(String url){
            return this.url.equals(url);
        }

        boolean isValid() throws SQLException{
            return !conn.isClosed();
        }
    }
}
