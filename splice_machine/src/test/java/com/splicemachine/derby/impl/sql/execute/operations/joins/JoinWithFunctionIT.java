package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.sparkproject.guava.collect.Lists;

import java.sql.ResultSet;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by jyuan on 5/18/16.
 */
@RunWith(Parameterized.class)
public class JoinWithFunctionIT extends SpliceUnitTest {
    private static final String SCHEMA = JoinWithFunctionIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(4);
        params.add(new Object[]{"NESTEDLOOP"});
        params.add(new Object[]{"SORTMERGE"});
        params.add(new Object[]{"BROADCAST"});
        return params;
    }
    private String joinStrategy;

    public JoinWithFunctionIT(String joinStrategy) {
        this.joinStrategy = joinStrategy;
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("create table a (i int, j int)")
                .withInsert("insert into a values(?,?)")
                .withRows(rows(row(1, 1), row(2, 2), row(-3, 3), row(-4, 4))).create();

        new TableCreator(spliceClassWatcher.getOrCreateConnection())
                .withCreate("create table b (i int, j int)")
                .withInsert("insert into b values(?,?)")
                .withRows(rows(row(-1, 1), row(-2, 2), row(3, 3), row(4, 4))).create();
    }

    @Test
    public void testAbsolute() throws Exception {
        String sql = String.format("select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "a\n" +
                ", b  --SPLICE-PROPERTIES joinStrategy=%s\n" +
                "where abs(a.i)=abs(b.i) order by a.i", joinStrategy);
        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected =
                "I | J | I | J |\n" +
                "----------------\n" +
                "-4 | 4 | 4 | 4 |\n" +
                "-3 | 3 | 3 | 3 |\n" +
                " 1 | 1 |-1 | 1 |\n" +
                " 2 | 2 |-2 | 2 |";
        assertEquals("\n"+sql+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
}
