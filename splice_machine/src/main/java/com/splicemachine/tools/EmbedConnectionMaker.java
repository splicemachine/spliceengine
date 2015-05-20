package com.splicemachine.tools;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.jdbc.EmbeddedDriver;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.db.AuthenticationConstants;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static java.lang.String.format;

/**
 * Provides connections for internal use.
 */
public final class EmbedConnectionMaker implements ConnectionPool.Supplier {

    private static final String JDBC_URL = format("jdbc:splice:%s;user=splice", SpliceConstants.SPLICE_DB);

    private final EmbeddedDriver driver = new EmbeddedDriver();

    /**
     * Connection for internal use. Connects as 'splice' without using a password.
     */
    @Override
    public Connection createNew() throws SQLException {
        return driver.connect(JDBC_URL, SpliceDriver.driver().getProperties());
    }

    /**
     * Intended to be called the very first time we connect to a newly initialized database.  This
     * method appears to be responsible for setting the initial password for our 'splice' user.
     */
    public Connection createFirstNew() throws SQLException {
        Properties properties = (Properties) SpliceDriver.driver().getProperties().clone();
        if (!AuthenticationConstants.authentication.equalsIgnoreCase("LDAP")) {
            properties.remove(EmbedConnection.INTERNAL_CONNECTION);
        }
        final String SPLICE_DEFAULT_PASSWORD = "admin";
        return driver.connect(JDBC_URL + ";create=true;password=" + SPLICE_DEFAULT_PASSWORD, properties);
    }
}
