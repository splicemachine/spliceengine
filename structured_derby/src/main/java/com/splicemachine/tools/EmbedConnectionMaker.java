package com.splicemachine.tools;

import com.splicemachine.derby.hbase.SpliceDriver;
import org.apache.derby.jdbc.EmbeddedDriver;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Scott Fines
 * Created on: 3/22/13
 */
public final class EmbedConnectionMaker implements ConnectionPool.Supplier {
    private static final String DRIVER_CLASS_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String protocol = "jdbc:derby:splice:";
    private static final String dbName = "wombat";

    private static final Class<EmbeddedDriver> driverClass;

    static{
        try {
            driverClass= (Class<EmbeddedDriver>) Class.forName(DRIVER_CLASS_NAME);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to load embedded drivers. " +
                    "Check classpath for correct jars",e);
        }
    }

    private final EmbeddedDriver driver;

    public EmbedConnectionMaker() {
        try {
            driver = loadDriver();
        } catch (Exception e) {
            throw new RuntimeException("Unable to create Embedded driver. " +
                    "Check classpath for correct jars",e);
        }
    }

    @Override
    public Connection createNew() throws SQLException {
        return driver.connect(protocol+dbName+";create=true",
                SpliceDriver.driver().getProperties());
    }

    /*private helper methods*/
    private EmbeddedDriver loadDriver() throws Exception{
        return driverClass.newInstance();
    }
}
