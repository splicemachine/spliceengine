package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PredicateWithSpecialCharactersIT extends SpliceUnitTest {

    private Boolean useSpark;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }

    public static final String CLASS_NAME = PredicateWithSpecialCharactersIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public PredicateWithSpecialCharactersIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    private static final String[] specialChars = {"''", "\\", "\""};

    @BeforeClass
    public static void createDataSet() throws Exception {
        spliceClassWatcher.execute("create table t1 (a1 char(50) not null)");
        spliceClassWatcher.execute(format("insert into t1 values %s",
                Arrays.stream(specialChars)
                        .map(s -> format("'a%sb'", s))
                        .collect(Collectors.joining(","))
        ));
        spliceClassWatcher.execute("create view v1 as select * from t1 union select * from t1");
    }

    @Test
    public void testCount() throws Exception {
        for (String specialChar: specialChars) {
            testQuery(format("SELECT COUNT(*) FROM v1 --splice-properties useSpark=true\n WHERE a1 = 'a%sb'", specialChar),
                    "1 |\n" +
                    "----\n" +
                    " 1 |", methodWatcher);
        }
    }
}