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
            this.conn = new TestConnection(SpliceNetConnection.newBuilder().user(user).password(pwd).build());
        }

        public boolean matches(String user){
            return this.user.equals(user);
        }

        boolean isValid() throws SQLException{
            return !conn.isClosed();
        }
    }
}
