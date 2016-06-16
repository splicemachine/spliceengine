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
