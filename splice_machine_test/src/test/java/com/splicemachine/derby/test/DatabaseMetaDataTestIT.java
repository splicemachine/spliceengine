package com.splicemachine.derby.test;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;

import java.sql.DatabaseMetaData;

public class DatabaseMetaDataTestIT {

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();


    @Test
    public void testVersionAndProductName() throws Exception {
        DatabaseMetaData dmd = methodWatcher.createConnection().getMetaData();
        Assert.assertEquals("Splice Machine", dmd.getDatabaseProductName());
        Assert.assertEquals("10.9.2.2 - (1)", dmd.getDatabaseProductVersion());
    }
}