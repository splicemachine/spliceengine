package org.apache.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import java.sql.ResultSet;

/**
 * @author Scott Fines
 *         Created on: 2/23/13
 */
public class MetatablesTest {
    private static final Logger LOG = Logger.getLogger(MetatablesTest.class);	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testSelectWithOr() throws Exception{
        /*
         * Set Bug #245 for details on the existence of this test.
         */
        String aliasTable = "SYSALIASES";
        String checksTable = "SYSCHECKS";
        ResultSet rs = methodWatcher.executeQuery("select * from sys.systables where tablename = 'SYSALIASES' or tablename = 'SYSCHECKS'");
        int count = 0;
        while(rs.next()){
            count++;
            String tableName = rs.getString(2);
            Assert.assertTrue("incorrect table returned!",aliasTable.equalsIgnoreCase(tableName)||checksTable.equalsIgnoreCase(tableName));
            LOG.info(String.format("table=%s",tableName));
        }
        Assert.assertEquals("Incorrect count!",2,count);
    }
}
