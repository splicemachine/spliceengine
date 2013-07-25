package com.splicemachine.test.connection;

import java.sql.Connection;

/**
 * Currently unused.
 *
 * @author Jeff Cunningham
 *         Date: 7/23/13
 */
public interface ConnectionPool {
    Connection getConnection() throws Exception;

    void returnConnection(Connection connection) throws Exception;
}
