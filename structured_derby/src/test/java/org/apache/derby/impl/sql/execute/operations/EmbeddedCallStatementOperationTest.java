package org.apache.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.DerbyTestRule;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Collections;

/**
 * @author Scott Fines
 *         Created on: 3/8/13
 */
public class EmbeddedCallStatementOperationTest {
    private static final Logger LOG = Logger.getLogger(EmbeddedCallStatementOperationTest.class);

    @Rule
    public DerbyTestRule rule = new DerbyTestRule(Collections.singletonMap("t", "i int"),LOG);

    @BeforeClass
    public static void startup() throws Exception{
        DerbyTestRule.start();
    }

    @AfterClass
    public static void shutdown() throws Exception{
        DerbyTestRule.shutdown();
    }

    @Test
    public void testCallGetIndexInfo() throws Exception{
        DatabaseMetaData dmd = rule.getConnection().getMetaData();
        ResultSet resultSet = dmd.getIndexInfo(null, "SYS", "SYSSCHEMAS", false, true);
        while(resultSet.next()){
            LOG.info(resultSet.getString(1));
        }
    }

}
