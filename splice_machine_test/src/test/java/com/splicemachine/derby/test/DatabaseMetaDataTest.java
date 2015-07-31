package com.splicemachine.derby.test;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.sql.DatabaseMetaData;

/**
 * Created by dmustafin on 7/30/15.
 */
public class DatabaseMetaDataTest {

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();


    @Test
    public void testVersion() throws Exception {
        DatabaseMetaData dmd = methodWatcher.createConnection().getMetaData();
        Assert.assertEquals("Apache Derby", dmd.getDatabaseProductName());
    }
}