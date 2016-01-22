package com.splicemachine.derby.impl.load;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.pipeline.exception.ErrorState;

/**
 * Tests for checking proper error conditions hold for Importing.
 *
 * That is, if an import fails, it should fail with certain characteristics (depending on
 * the failure type). This test exists to check those characteristics
 *
 * @author Scott Fines
 * Created on: 9/20/13
 */
public class ImportErrorIT extends SpliceUnitTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = ImportErrorIT.class.getSimpleName().toUpperCase();
    /*
     * a Single table used to test all the different errors:
     *
     * null into a not null X
     * String too long for field X
     *
     * Long into int overflow X
     * Float into int truncation error X
     * Double into long truncation error X
     * Double into Float overflow X
     *
     * incorrect datetime format X
     * incorrect time format X
     * incorrect date format X
     *
     * File not found X
     * No such table
     * cannot modify an auto-increment column
     */
    private static final String TABLE = "errorTable";

    private final String SE009 = ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.getSqlState();

    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);
    private static final SpliceTableWatcher tableWatcher = new SpliceTableWatcher(TABLE,schema.schemaName,"(a int not null, b bigint, c real, d double, e varchar(5),f date not null,g time not null, h timestamp not null)");
    private static final SpliceTableWatcher decimalTable = new SpliceTableWatcher("DECIMALTABLE", schema.schemaName, "(d decimal(2))");
    private static final SpliceTableWatcher incrementTable = new SpliceTableWatcher("INCREMENT", schema.schemaName, "(a int generated always as identity, b int)");

    private static final SpliceTableWatcher constraintTable = new SpliceTableWatcher("CONSTRAINT_TABLE", schema.schemaName, "(i int check (i<10))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(schema)
                                            .around(tableWatcher)
                                            .around(decimalTable)
                                            .around(incrementTable)
                                            .around(constraintTable);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static File BADDIR;
    @BeforeClass
    public static void beforeClass() throws Exception {
        BADDIR = ImportTestUtils.createBadLogDirectory(schema.schemaName);
    }

    @Test
    public void testNoSuchTable() throws Exception {
        runImportTest("NO_SUCH_TABLE","file_doesnt_exist.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", "XIE0M",se.getSQLState());

                String correctErrorMessage = "Table '"+table+"' does not exist.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage().trim());
            }
        });

    }

    @Test
    public void testCannotFindFile() throws Exception {
        runImportTest("file_doesnt_exist.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!","XIE04",se.getSQLState());

                //se.getMessage() can return message either with a period or without a period.  So we check for each.
                String correctErrorMessage = String.format("Data file not found: File %s does not exist",location);
                String correctErrorMessage1 = String.format("Data file not found: File %s does not exist.",location);
                String retval = se.getMessage();
                Assert.assertTrue("Incorrect error message! location={" + location + "}, retval={" + retval + "}",
                        retval.equals(correctErrorMessage) || retval.equals(correctErrorMessage1));
//                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });

    }

    @Test
    public void testCannotInsertNullFieldIntoNonNullColumn() throws Exception {
        runImportTest("null_col.csv", new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", "23502", se.getSQLState());

                String correctErrorMessage = "Column 'A'  cannot accept a NULL value.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test
    public void testCannotInsertStringOverCharacterLimits() throws Exception {
        runImportTest("long_string.csv",new ErrorCheck(){
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", "22001", se.getSQLState());

                String correctErrorMessage = "A truncation error was encountered trying to shrink VARCHAR " +
                    "'thisstringhasmorethanfivecharacters' to length 5.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test @Ignore("DB-4342")
    public void testCannotInsertALongIntoAnIntegerField() throws Exception {
        //  FIXME: JC - java exception burried in an "unexpected exception"
        runImportTest("long_int.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                System.err.println(ImportTestUtils.printMsgSQLState("testCannotInsertALongIntoAnIntegerField",se));
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"Invalid character string format for type INTEGER.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test @Ignore("DB-4342")
    public void testCannotInsertAFloatIntoAnIntegerField() throws Exception {
        //  FIXME: JC - java exception burried in an "unexpected exception"
        runImportTest("float_int.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                System.err.println(ImportTestUtils.printMsgSQLState("testCannotInsertAFloatIntoAnIntegerField",se));
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"Invalid character string format for type INTEGER.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test @Ignore("DB-4342")
    public void testCannotInsertADoubleIntoALongField() throws Exception {
        //  FIXME: JC - java exception burried in an "unexpected exception"
        runImportTest("double_long.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                System.err.println(ImportTestUtils.printMsgSQLState("testCannotInsertADoubleIntoALongField",se));
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"Invalid character string format for type BIGINT.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test @Ignore("DB-4342, DB-4341")
    public void testCannotInsertADoubleIntoAFloatField() throws Exception {
        //  FIXME: JC - Import allows decimal of the wrong range inserted into a table, then can't query.
        runImportTest("double_float.csv", new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                System.err.println(ImportTestUtils.printMsgSQLState("testCannotInsertADoubleIntoAFloatField",se));
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", "22003", se.getSQLState());

                String correctErrorMessage = "The resulting value is outside the range for the data type REAL.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test @Ignore("DB-4342")
    public void testCannotInsertAPoorlyFormattedDate() throws Exception {
        //  FIXME: JC - java exception burried in an "unexpected exception"
        runImportTest("bad_date.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                System.err.println(ImportTestUtils.printMsgSQLState("testCannotInsertAPoorlyFormattedDate",se));
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"The syntax of the string representation of a datetime value is incorrect.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test @Ignore("DB-4342")
    public void testCannotInsertAPoorlyFormattedTime() throws Exception {
        //  FIXME: JC - java exception burried in an "unexpected exception"
        runImportTest("bad_time.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                System.err.println(ImportTestUtils.printMsgSQLState("testCannotInsertAPoorlyFormattedTime",se));
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"The syntax of the string representation of a datetime value is incorrect.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test @Ignore("DB-4342")
    public void testCannotInsertNullDateIntoDateField() throws Exception{
        //  FIXME: JC - java exception burried in an "unexpected exception"
        runImportTest("null_date.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                System.err.println(ImportTestUtils.printMsgSQLState("testCannotInsertNullDateIntoDateField",se));
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test
    public void testCannotInsertNullTimeIntoTimeField() throws Exception{
        runImportTest("null_time.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", "23502", se.getSQLState());

                String correctErrorMessage = "Column 'G'  cannot accept a NULL value.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test
    public void testCannotInsertNullTimestampIntoTimestampField() throws Exception{
        runImportTest("null_timestamp.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", "23502", se.getSQLState());

                String correctErrorMessage = "Column 'H'  cannot accept a NULL value.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    /**
     * DB-3937
     * @throws Exception
     */
    @Test
    public void testCannotInsertValueOutOfConstraint() throws Exception{

        String table = constraintTable.tableName;
        String file = "field_with_constraint.csv";

        String inputFilePath = getResourceDirectory()+"import/"+file;
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
                        "'%s'," +  // bad record dir
                        "'true'," +  // has one line records
                        "null)",   // char set
                schema.schemaName, table, inputFilePath,
                0, BADDIR.getCanonicalPath()));

        ps.execute();

        Path badFile = Paths.get(BADDIR.getCanonicalPath() + "/" + file + ".bad");

        if (!Files.exists(badFile)) {
            Assert.fail("No SQLException was thrown!");
        }
    }

    @Test @Ignore("DB-4342")
    public void testCannotInsertAPoorlyFormatedTimestamp() throws Exception {
        //  FIXME: JC - java exception burried in an "unexpected exception"
        runImportTest("bad_timestamp.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                System.err.println(ImportTestUtils.printMsgSQLState("testCannotInsertAPoorlyFormatedTimestamp",se));
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"The syntax of the string representation of a datetime value is incorrect.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test @Ignore("DB-4341")
    public void testDecimalTable() throws Exception {
        //  FIXME: JC - java exception burried in an "unexpected exception"
        runImportTest("DECIMALTABLE","bad_decimal.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009 ,se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"The resulting value is outside the range for the data type DECIMAL/NUMERIC(2,0).";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    /*
		@Test(expected = SQLException.class)
		public void testNonNullIntoIncrement() throws Exception {
				runImportTest("INCREMENT","simple_column.txt",new ErrorCheck() {
						@Override
						public void check(String table, String location, SQLException se) {
								//make sure the error code is correct
								Assert.assertEquals("Incorrect sql state!",ErrorState.LANG_AI_CANNOT_MODIFY_AI.getSqlState(),se.getSQLState());

								String correctErrorMessage = "Attempt to modify an identity column 'A'.";
								Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage().trim());
						}
				});
		}
    */
    private interface ErrorCheck{
        void check(String table, String location, SQLException se);
    }

    private void runImportTest(String file, ErrorCheck check) throws Exception {
        runImportTest(TABLE,file,check);
    }

    /**
     * Verifies that an import of the specified file into the specified table throws SQLException, then performs
     * further assertions in ErrorCheck
     */
    private void runImportTest(String table,String file,ErrorCheck check) throws Exception {
        String inputFilePath = getResourceDirectory()+"test_data/bad_import/"+file;
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
                                                                         "'%s'," +  // bad record dir
                                                                         "null," +  // has one line records
                                                                         "null)",   // char set
                                                                     schema.schemaName, table, inputFilePath,
                                                                     0, BADDIR.getCanonicalPath()));

        try{
            ps.execute();
            Assert.fail("No SQLException was thrown!");
        }catch(SQLException se){
            check.check(schema.schemaName+"."+table, inputFilePath, se);
        }
    }

}
