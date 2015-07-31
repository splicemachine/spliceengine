package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;

import java.sql.ResultSet;
/**
 * Created by jyuan on 7/30/15.
 */
public class AnalyzeTableIT {
    private static final String SCHEMA = AnalyzeTableIT.class.getSimpleName();
    private static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createTables() throws Exception {
        classWatcher.executeUpdate("create table T1 (I INT)");
        classWatcher.executeUpdate("create table T2 (I INT)");

        classWatcher.executeUpdate("insert into T1 values 1, 2, 3");
        classWatcher.executeUpdate("insert into T2 values 1, 2, 3, 4, 5, 6, 7, 8, 9");
    }

    @Test
    public void testAnalyzeTable() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("analyze table AnalyzeTableIT.T1");
        int count = 0;
        while(rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void testAnalyzeSchema() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("analyze schema AnalyzeTableIT");
        int count = 0;
        while(rs.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
    }
}
