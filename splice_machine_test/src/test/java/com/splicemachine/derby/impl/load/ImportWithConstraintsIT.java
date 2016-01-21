package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.test.framework.*;
import org.apache.commons.dbutils.DbUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Integration tests to check constrains on import
 *
 * @author Andrey Paslavsky
 */
public class ImportWithConstraintsIT extends SpliceUnitTest {
    private static final String CLASS_NAME = ImportWithConstraintsIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME) {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            Connection connection = null;
            Statement statement = null;
            try {
                connection = SpliceNetConnection.getConnection();
                statement = connection.createStatement();
                statement.execute(format("DROP TABLE IF EXISTS %s.%s", CLASS_NAME, TABLE_WITH_FK));
                statement.execute(format("DROP TABLE IF EXISTS %s.%s", CLASS_NAME, TEMP_TABLE));
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                DbUtils.closeQuietly(statement);
                DbUtils.commitAndCloseQuietly(connection);
            }
        }
    };

    private static final String TABLE_NOTNULL = "T1";
    private static final String TABLE_WITH_PK = "T2";
    private static final String TABLE_WITH_UNIQUE_COLUMN = "T3";
    private static final String TEMP_TABLE = "T4";
    private static final String TABLE_WITH_FK = "T5";
    private static final String TABLE_WITH_CHECK = "T6";
    private static final String TABLE_WITH_TRIGGER = "T7";
    private static final String TEMP_TABLE2 = "T8";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).around(schema).
            around(new SpliceTableWatcher(TABLE_NOTNULL, schema.schemaName, "(a int not null, some_text varchar(100))")).
            around(new SpliceTableWatcher(TABLE_WITH_UNIQUE_COLUMN, schema.schemaName, "(a int constraint T3_UNIQUE UNIQUE, some_text varchar(100))")).
            around(new SpliceTableWatcher(TABLE_WITH_PK, schema.schemaName, "(a int not null constraint T2_PK primary key, some_text varchar(100))")).
            around(new SpliceTableWatcher(TEMP_TABLE, schema.schemaName, "(a int constraint T4_UNIQUE UNIQUE, some_text varchar(100))")).
            around(new SpliceTableWatcher(TABLE_WITH_FK, schema.schemaName, "(a int REFERENCES " + schema.schemaName + ".T4(a), some_text varchar(100))")).
            around(new SpliceTableWatcher(TABLE_WITH_CHECK, schema.schemaName, "(a int CONSTRAINT T5_CK CHECK (a >= 10), some_text varchar(100))")).
            around(new SpliceTableWatcher(TEMP_TABLE2, schema.schemaName, "(temp_msg varchar(100))")).
            around(new SpliceTableWatcher(TABLE_WITH_TRIGGER, schema.schemaName, "(a int, some_text varchar(100))") {
                @Override
                public void start() {
                    super.start();
                    try {
                        spliceClassWatcher.executeUpdate(format(
                                "CREATE TRIGGER %s_TRIGGER AFTER INSERT ON %s.%s FOR EACH STATEMENT INSERT INTO %s.%s VALUES ('OK!')",
                                tableName, schemaName, tableName, schemaName, TEMP_TABLE2));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private static String badDirPath;

    @BeforeClass
    public static void beforeClass() throws Exception {
        badDirPath = ImportTestUtils.createBadLogDirectory(schema.schemaName).getCanonicalPath();
//        TemporaryFolder temporaryFolder = new TemporaryFolder();
//        temporaryFolder.create();
//        badDirPath = temporaryFolder.newFolder(schema.schemaName).getCanonicalPath();
    }

    @Test
    public void testImportNotNullData() throws Exception {
        Assert.assertTrue(runImport(TABLE_NOTNULL, "notNull.csv"));
    }

    @Test(expected = SQLException.class)
    public void testImportWithNullData() throws Exception {
        runImport(TABLE_NOTNULL, "hasNull.csv");
        Assert.fail();
    }

    @Test
    public void testImportIntoTableWithPK() throws Exception {
        Assert.assertTrue(runImport(TABLE_WITH_PK, "uniqueFirstColumn.csv"));
    }

    @Test(expected = SQLException.class)
    public void testFailImportIntoTableWithPK() throws Exception {
        runImport(TABLE_WITH_PK, "duplicateRows.csv");
        Assert.fail();
    }

    @Test(expected = SQLException.class)
    public void testImportNullIntoTableWithPK() throws Exception {
        runImport(TABLE_WITH_PK, "hasNull.csv");
        Assert.fail();
    }

    @Test
    public void testImportIntoTableWithUniqueColumn() throws Exception {
        Assert.assertTrue(runImport(TABLE_WITH_UNIQUE_COLUMN, "uniqueFirstColumn.csv"));
    }

    @Test(expected = SQLException.class)
    public void testFailImportIntoTableWithUniqueColumn() throws Exception {
        runImport(TABLE_WITH_UNIQUE_COLUMN, "duplicateRows.csv");
        Assert.fail();
    }

    @Test
    public void testImportIntoTableWithForeignKey() throws Exception {
        Assert.assertTrue(runImport(TEMP_TABLE, "uniqueFirstColumn.csv"));
        Assert.assertTrue(runImport(TABLE_WITH_FK, "fk.csv"));
    }

    @Test(expected = SQLException.class)
    public void testFailImportIntoTableWithForeignKey() throws Exception {
        runImport(TABLE_WITH_FK, "fkNotExists.csv");
        Assert.fail();
    }

    @Test
    public void testImportIntoTableWithCheckCondition() throws Exception {
        Assert.assertTrue(runImport(TABLE_WITH_CHECK, "moreThan10.csv"));
    }

    @Test(expected = SQLException.class)
    public void testFailImportIntoTableWithCheckCondition() throws Exception {
        runImport(TABLE_WITH_CHECK, "checkFail.csv");
        Assert.fail();
    }

    @Test
    public void testImportIntoTableWithTrigger() throws Exception {
        Assert.assertTrue(runImport(TABLE_WITH_TRIGGER, "notNull.csv"));
        firstRowContainsQuery(format("select * from %s.%s", schema.schemaName, TEMP_TABLE2), "OK!", methodWatcher);
    }


    private boolean runImport(String table, String file) throws Exception {
        String inputFilePath = getResourceDirectory() + "import/verify-constraints/" + file;

        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA(" +
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
//                        "'%s')",   // bad record dir
                        "'%s'," +  // bad record dir
                        "null," +  // has one line records
                        "null)",   // char set
                schema.schemaName, table, inputFilePath,
                0, badDirPath));
        return ps.execute();
    }
}
