package com.splicemachine.derby.client;

import org.junit.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLNonTransientConnectionException;
import java.util.Properties;

/**
 * Created by jleach on 12/13/16.
 */
public class TestUnknownClient {

    @Test (expected = SQLNonTransientConnectionException.class)
    public void testDerbyDriverFails() throws Exception {
        Driver derbyDriver = DriverManager.getDriver("jdbc:derby://localhost:1527/splicedb;create=true;user=hmm;password=splice;create=admin");
        derbyDriver.connect("jdbc:derby://localhost:1527/splicedb;create=true;user=splice;password=admin",new Properties());
    }

}
