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

import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Date: 6/21/16
 */
public class TestConnectionPool{
    private ConnectionHolder currentConnection;

    public TestConnection getConnection(String user, String pwd) throws SQLException{
        if(currentConnection!=null){
            if(currentConnection.isValid() && currentConnection.matches(user))
                return currentConnection.conn;

            currentConnection.conn.close();
        }
        currentConnection = new ConnectionHolder(user,pwd);
        return currentConnection.conn;
    }

    private static class ConnectionHolder{
        final TestConnection conn;
        private final String  user;

        public ConnectionHolder(String user,String pwd) throws SQLException{
            this.user=user;
            this.conn = new TestConnection(SpliceNetConnection.getConnectionAs(user,pwd));
        }

        public boolean matches(String user){
            return this.user.equals(user);
        }

        boolean isValid() throws SQLException{
            return !conn.isClosed();
        }
    }
}
