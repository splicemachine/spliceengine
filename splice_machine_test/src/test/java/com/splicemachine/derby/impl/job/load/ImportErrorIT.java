package com.splicemachine.derby.impl.job.load;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.pipeline.exception.ErrorState;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

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
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schema)
						.around(tableWatcher)
						.around(decimalTable)
						.around(incrementTable);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testNoSuchTable() throws Exception {
        runImportTest("no_such_table","file_doesn't_exist.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                Assert.assertEquals("Incorrect sql state!", "XIE0M",se.getSQLState());
            }
        });
    }

    @Test
    public void testCannotFindFile() throws Exception {
        runImportTest("file_doesn't_exist.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!","XIE04",se.getSQLState());
            }
        });
    }

    @Test
    public void testCannotInsertNullFieldIntoNonNullColumn() throws Exception {
        runImportTest("null_col.csv", new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                Assert.assertEquals("Incorrect sql state!", SQLState.LANG_NULL_INTO_NON_NULL , se.getSQLState());
            }
        });
    }

    @Test
    public void testCannotInsertStringOverCharacterLimits() throws Exception {
        runImportTest("long_string.csv",new ErrorCheck(){
            @Override
            public void check(String table, String location, SQLException se) {
                Assert.assertEquals("Incorrect sql state!", SQLState.LANG_STRING_TRUNCATION, se.getSQLState());
            }
        });
    }

    @Test
    public void testCannotInsertALongIntoAnIntegerField() throws Exception {
        runImportTest("long_int.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"Invalid character string format for type INTEGER.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test
    public void testCannotInsertAFloatIntoAnIntegerField() throws Exception {
        runImportTest("float_int.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"Invalid character string format for type INTEGER.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test
    public void testCannotInsertADoubleIntoALongField() throws Exception {
        runImportTest("double_long.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"Invalid character string format for type BIGINT.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test
    @Ignore
    public void testCannotInsertADoubleIntoAFloatField() throws Exception {
        runImportTest("double_float.csv", new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", "22003", se.getSQLState());

                String correctErrorMessage = "The resulting value is outside the range for the data type REAL.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test
    public void testCannotInsertAPoorlyFormattedDate() throws Exception {
        runImportTest("bad_date.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct

                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"The syntax of the string representation of a datetime value is incorrect.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test
    public void testCannotInsertAPoorlyFormattedTime() throws Exception {
        runImportTest("bad_time.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"The syntax of the string representation of a datetime value is incorrect.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

		@Test
		public void testCannotInsertNullDateIntoDateField() throws Exception{
				runImportTest("null_date.csv",new ErrorCheck() {
						@Override
						public void check(String table, String location, SQLException se) {
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
								Assert.assertEquals("Incorrect sql state!", SQLState.LANG_NULL_INTO_NON_NULL, se.getSQLState());
						}
				});
		}

		@Test
		public void testCannotInsertNullTimestampIntoTimestampField() throws Exception{
				runImportTest("null_timestamp.csv",new ErrorCheck() {
						@Override
						public void check(String table, String location, SQLException se) {
								Assert.assertEquals("Incorrect sql state!", SQLState.LANG_NULL_INTO_NON_NULL, se.getSQLState());
						}
				});
		}

    @Test
    public void testCannotInsertAPoorlyFormatedTimestamp() throws Exception {
        runImportTest("bad_timestamp.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", SE009, se.getSQLState());

                String correctErrorMessage = "Too many bad records in import"; //"The syntax of the string representation of a datetime value is incorrect.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test
    public void testDecimalTable() throws Exception {
        runImportTest("DECIMALTABLE","bad_decimal.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                Assert.assertEquals("Incorrect sql state!", SE009 ,se.getSQLState());
            }
        });
    }

    private interface ErrorCheck{
        void check(String table, String location, SQLException se);
    }

    private void runImportTest(String file, ErrorCheck check) throws SQLException {
        runImportTest(TABLE,file,check);
    }

    /**
     * Verifies that an import of the specified file into the specified table throws SQLException, then performs
     * further assertions in ErrorCheck
     */
    private void runImportTest(String table,String file,ErrorCheck check) throws SQLException {
        String location = getResourceDirectory()+"test_data/bad_import/"+file;
        PreparedStatement ps = null;
        try{
            ps = methodWatcher.prepareStatement("call SYSCS_UTIL.IMPORT_DATA(?, ?, ?, ?, ',', null, null, null, null, 0, null, true, null)");
            ps.setString(1, schema.schemaName);
            ps.setString(2, table);
            ps.setNull(3, Types.VARCHAR);
            ps.setString(4, location);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail("Incorrect setup for prepared statement");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Incorrect setup for prepared statement");
        }

        try{
            ps.execute();
            Assert.fail("No SQLException was thrown!");
        }catch(SQLException se){
            check.check(table, location, se);
        }
    }

}
