package com.splicemachine.test.connection;

import java.util.Properties;
import org.apache.log4j.Logger;

/**
 * @author Jeff Cunningham
 *         Date: 10/23/13
 */
public abstract class BaseConnection {
    private static final Logger LOG = Logger.getLogger(BaseConnection.class);

    protected static String embeddedDriver = "org.apache.derby.jdbc.EmbeddedDriver";
    protected static String clientDriver = "org.apache.derby.jdbc.ClientDriver";
    protected static String create = ";create=true";
    protected static Properties props = new Properties();

    protected static boolean loaded;

    protected static synchronized void loadDriver(String driver) throws Exception{
        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException e) {
            String msg = "\nUnable to load the JDBC driver " + driver + " Please check your CLASSPATH.";
            LOG.error(msg, e);
            throw e;
        } catch (InstantiationException e) {
            String msg = "\nUnable to instantiate the JDBC driver " + driver;
            LOG.error(msg, e);
            throw e;
        } catch (IllegalAccessException e) {
            String msg = "\nNot allowed to access the JDBC driver " + driver;
            LOG.error(msg, e);
            throw e;
        }
        loaded =  true;
    }
}
