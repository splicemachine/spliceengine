package com.splicemachine.derby.impl.sql.execute.actions;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

/**
 * @author Scott Fines
 *         Date: 1/19/16
 */
public class TempTableHBaseIT{

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();
    /**
     * Make sure the HBase table that backs a Splice temp table gets cleaned up at the end of the user session.
     * @throws Exception
     */
    @Test
    public void testTempHBaseTableGetsDropped() throws Exception {
        long start = System.currentTimeMillis();
        HBaseAdmin hBaseAdmin = new HBaseAdmin()
        String tempConglomID;
        boolean hbaseTempExists;
        final String tmpCreate = "DECLARE GLOBAL TEMPORARY TABLE %s.%s %s not logged on commit preserve rows";
        try (Connection connection = methodWatcher.createConnection()) {
            SQLClosures.execute(connection, new SQLClosures.SQLAction<Statement>() {
                @Override
                public void execute(Statement statement) throws Exception {
                    statement.execute(String.format(tmpCreate, tableSchema.schemaName, SIMPLE_TEMP_TABLE, simpleDef));
                    SpliceUnitTest.loadTable(statement, tableSchema.schemaName + "." + SIMPLE_TEMP_TABLE, empNameVals);
                }
            });
            connection.commit();
            tempConglomID = TestUtils.lookupConglomerateNumber(tableSchema.schemaName, SIMPLE_TEMP_TABLE, methodWatcher);
            hbaseTempExists = hBaseAdmin.tableExists(tempConglomID);
            Assert.assertTrue("HBase temp table ["+tempConglomID+"] does not exist.", hbaseTempExists);
        }  finally {
            methodWatcher.closeAll();
        }
        hbaseTempExists = hBaseAdmin.tableExists(tempConglomID);
        if (hbaseTempExists) {
            // HACK: wait a sec, try again.  It's going away, just takes some time.
            Thread.sleep(1000);
            hbaseTempExists = hBaseAdmin.tableExists(tempConglomID);
        }
        Assert.assertFalse("HBase temp table ["+tempConglomID+"] still exists.",hbaseTempExists);
        System.out.println("HBase Table check took: "+TestUtils.getDuration(start, System.currentTimeMillis()));
    }
}
