package com.splicemachine.derby.impl.job.load;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.utils.ErrorState;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
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
public class ImportErrorIT {
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

    @Test(expected = SQLException.class)
    public void testNoSuchTable() throws Exception {
        runImportTest("no_such_table","file_doesn't_exist.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", ErrorState.LANG_TABLE_NOT_FOUND.getSqlState(),se.getSQLState());

                String correctErrorMessage = "Table/View '"+table.toUpperCase()+"' does not exist.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });

    }

    @Test(expected=SQLException.class)
    public void testCannotFindFile() throws Exception {
        runImportTest("file_doesn't_exist.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!","XIE04",se.getSQLState());

                String correctErrorMessage = String.format("Data file not found: File %s does not exist",location);
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });

    }

    @Test(expected = SQLException.class)
    public void testCannotInsertNullFieldIntoNonNullColumn() throws Exception {
        runImportTest("null_col.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!","23502",se.getSQLState());

                String correctErrorMessage = "Column 'A'  cannot accept a NULL value.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test(expected = SQLException.class)
    public void testCannotInsertStringOverCharacterLimits() throws Exception {
        runImportTest("long_string.csv",new ErrorCheck(){
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!","22001",se.getSQLState());

                String correctErrorMessage = "A truncation error was encountered trying to shrink VARCHAR " +
                        "'thisstringhasmorethanfivecharacters' to length 5.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test(expected = SQLException.class,timeout=5000)
    public void testCannotInsertALongIntoAnIntegerField() throws Exception {
        runImportTest("long_int.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!","22018",se.getSQLState());

                String correctErrorMessage = "Invalid character string format for type INTEGER.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test(expected = SQLException.class)
    public void testCannotInsertAFloatIntoAnIntegerField() throws Exception {
        runImportTest("float_int.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!","22018",se.getSQLState());

                String correctErrorMessage = "Invalid character string format for type INTEGER.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test(expected = SQLException.class)
    public void testCannotInsertADoubleIntoALongField() throws Exception {
        runImportTest("double_long.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!","22018",se.getSQLState());

                String correctErrorMessage = "Invalid character string format for type BIGINT.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test(expected = SQLException.class)
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

    @Test(expected =  SQLException.class)
    public void testCannotInsertAPoorlyFormattedDate() throws Exception {
        runImportTest("bad_date.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", "22007", se.getSQLState());

                String correctErrorMessage = "The syntax of the string representation of a datetime value is incorrect.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test(expected =  SQLException.class)
    public void testCannotInsertAPoorlyFormattedTime() throws Exception {
        runImportTest("bad_time.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", "22007", se.getSQLState());

                String correctErrorMessage = "The syntax of the string representation of a datetime value is incorrect.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

		@Test(expected = SQLException.class)
		public void testCannotInsertNullDateIntoDateField() throws Exception{
				runImportTest("null_date.csv",new ErrorCheck() {
						@Override
						public void check(String table, String location, SQLException se) {
								//make sure the error code is correct
								Assert.assertEquals("Incorrect sql state!","23502",se.getSQLState());

								String correctErrorMessage = "Column 'F'  cannot accept a NULL value.";
								Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
						}
				});
		}

		@Test(expected = SQLException.class)
		public void testCannotInsertNullTimeIntoTimeField() throws Exception{
				runImportTest("null_time.csv",new ErrorCheck() {
						@Override
						public void check(String table, String location, SQLException se) {
								//make sure the error code is correct
								Assert.assertEquals("Incorrect sql state!","23502",se.getSQLState());

								String correctErrorMessage = "Column 'G'  cannot accept a NULL value.";
								Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
						}
				});
		}

		@Test(expected = SQLException.class)
		public void testCannotInsertNullTimestampIntoTimestampField() throws Exception{
				runImportTest("null_timestamp.csv",new ErrorCheck() {
						@Override
						public void check(String table, String location, SQLException se) {
								//make sure the error code is correct
								Assert.assertEquals("Incorrect sql state!","23502",se.getSQLState());

								String correctErrorMessage = "Column 'H'  cannot accept a NULL value.";
								Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
						}
				});
		}

    @Test(expected =  SQLException.class)
    public void testCannotInsertAPoorlyFormatedTimestamp() throws Exception {
        runImportTest("bad_timestamp.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", "22007", se.getSQLState());

                String correctErrorMessage = "The syntax of the string representation of a datetime value is incorrect.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test(expected = SQLException.class)
    public void testDecimalTable() throws Exception {
        runImportTest("DECIMALTABLE","bad_decimal.csv",new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!",ErrorState.LANG_OUTSIDE_RANGE_FOR_DATATYPE.getSqlState(),se.getSQLState());

                String correctErrorMessage = "The resulting value is outside the range for the data type DECIMAL/NUMERIC(2,0).";
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

    public void runImportTest(String file, ErrorCheck check) throws SQLException {
        runImportTest(TABLE,file,check);
    }

    public void runImportTest(String table,String file,ErrorCheck check) throws SQLException {
        String location = getResourceDirectory()+"/test_data/bad_import/"+file;
        PreparedStatement ps = null;
        try{
            ps = methodWatcher.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA(?,?,?,null, ?,',',null,null,null,null)");
            ps.setString(1,schema.schemaName);
            ps.setString(2,table);
            ps.setNull(3, Types.VARCHAR);
            ps.setString(4,location);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail("Incorrect setup for prepared statement");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Incorrect setup for prepared statement");
        }

        try{
            ps.execute();
        }catch(SQLException se){
            check.check(table, location, se);

            throw se;
        }
        Assert.fail("No error was returned!");
    }

    public static String getBaseDirectory() {
        String userDir = System.getProperty("user.dir");
        if(!userDir.endsWith("structured_derby"))
            userDir = userDir+"/structured_derby";
        return userDir;
    }
    public static String getResourceDirectory() {
        return getBaseDirectory()+"/src/test/resources";
    }
}
