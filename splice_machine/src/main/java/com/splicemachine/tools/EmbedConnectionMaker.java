package com.splicemachine.tools;

import com.splicemachine.SQLConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.jdbc.EmbeddedDriver;
import com.splicemachine.derby.impl.db.AuthenticationConfiguration;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;


/**
 * Provides connections for internal use.
 */
public final class EmbedConnectionMaker {

    private static final String JDBC_URL = String.format("jdbc:splice:%s;user=splice", SQLConfiguration.SPLICE_DB);

    private final EmbeddedDriver driver = new EmbeddedDriver();

    /**
     * Connection for internal use. Connects as 'splice' without using a password.
     */
//    @Override
    public Connection createNew(Properties dbProperties) throws SQLException {
        return driver.connect(JDBC_URL,dbProperties);
    }

    /**
     * Intended to be called the very first time we connect to a newly initialized database.  This
     * method appears to be responsible for setting the initial password for our 'splice' user.
     */
    public Connection createFirstNew(SConfiguration configuration,Properties dbProperties) throws SQLException {
        Properties properties = (Properties)dbProperties.clone();
        String auth = configuration.getString(AuthenticationConfiguration.AUTHENTICATION);
        if (!"LDAP".equalsIgnoreCase(auth)){
            properties.remove(EmbedConnection.INTERNAL_CONNECTION);
        }
        final String SPLICE_DEFAULT_PASSWORD = "admin";
        return driver.connect(JDBC_URL + ";create=true;password=" + SPLICE_DEFAULT_PASSWORD, properties);
    }
}
