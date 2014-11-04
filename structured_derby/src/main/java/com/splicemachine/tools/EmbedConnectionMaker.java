package com.splicemachine.tools;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;

import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.jdbc.EmbeddedDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author Scott Fines
 * Created on: 3/22/13
 */
public final class EmbedConnectionMaker implements ConnectionPool.Supplier {
    private static final String DRIVER_CLASS_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String protocol = "jdbc:derby:splice:";

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
        return driver.connect(protocol+SpliceConstants.SPLICE_DB+";create=true;user=splice;password=admin",
                SpliceDriver.driver().getProperties());
    }
    
    public Connection createFirstNew() throws SQLException {
    	Properties first = (Properties) SpliceDriver.driver().getProperties().clone();
    	if(!SpliceConstants.authentication.toUpperCase().equals("LDAP"))
    		first.remove(EmbedConnection.INTERNAL_CONNECTION);
    	
        return driver.connect(protocol+SpliceConstants.SPLICE_DB+";create=true;user=splice;password=admin",
                first);    	
    }

    /*private helper methods*/
    private EmbeddedDriver loadDriver() throws Exception{
        return driverClass.newInstance();
    }
}
