package com.splicemachine.derby.impl.load;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.getResourceDirectory;

import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * Created by yifu on 6/13/14.
 *
 */
public class ImportDefaultValueIT {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = ImportDefaultValueIT.class.getSimpleName().toUpperCase();
    protected static String TABLE_1 = "A";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_1,
                                                                                     spliceSchemaWatcher.schemaName,
                                                                                     "(COL1 INT, COL2 CHAR(3)DEFAULT'abc')");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Rule
    public TemporaryFolder baddir = new TemporaryFolder();

    @Test @Ignore("DB-4346")
    public void textImportDefaultValue() throws Exception{
        //  FIXME: JC - default values problem. File's empty column is null table column
        PrintWriter writer = new PrintWriter("/tmp/Test.txt","UTF-8");
        writer.println(",abc");
        writer.println("2,");
        writer.println("3,ab");
        writer.close();

        String baddirPath = baddir.newFolder().getCanonicalPath();
        PreparedStatement ps = methodWatcher.prepareStatement(String.format("call SYSCS_UTIL.IMPORT_DATA(" +
                                                                         "'%s'," +  // schema name
                                                                         "'%s'," +  // table name
                                                                         "null," +  // insert column list
                                                                         "'%s'," +  // file path
                                                                         "','," +   // column delimiter
                                                                         "null," +  // character delimiter
                                                                         "null," +  // timestamp format
                                                                         "null," +  // date format
                                                                         "null," +  // time format
                                                                         "%d," +    // max bad records
                                                                         "'%s'," +  // bad record dir
                                                                         "null," +  // has one line records
                                                                         "null)",                           // char set
                                                                            spliceSchemaWatcher.schemaName, TABLE_1,
                                                                            "/tmp/Test.txt",
                                                                            0, baddirPath));

        ps.execute();
        PreparedStatement s = methodWatcher.prepareStatement("select * from "+spliceSchemaWatcher.schemaName+"."+TABLE_1);
        List<String> expected  = Arrays.asList("abc","abc","ab");
        Collections.sort(expected);

        List<String> actual = Lists.newArrayListWithExpectedSize(expected.size());

        ResultSet rs = s.executeQuery();
        while(rs.next()){
            Assert.assertNotNull("2nd col of row with 1st col "+rs.getInt(1)+ " is null", rs.getString(2));
            actual.add(rs.getString(2));
        }
        Collections.sort(actual);
        Assert.assertEquals("Incorrect results!",expected,actual);
    }

}
