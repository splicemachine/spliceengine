package com.splicemachine.derby.impl.job.load;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.math.BigDecimal;
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
public class ImportErrorTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = ImportErrorTest.class.getSimpleName().toUpperCase();
    /*
     * a Single table used to test all the different errors:
     *
     * null into a not null
     * String too long for field
     *
     * Long into int overflow
     * Float into int truncation error
     * Double into long truncation error
     * Double into Float overflow
     *
     * incorrect datetime format
     * incorrect time format
     * incorrect date format
     *
     * String in int field
     * double in int field
     * float in int field
     */
    private static final String TABLE = "errorTable";

    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);
    private static final SpliceTableWatcher tableWatcher = new SpliceTableWatcher(TABLE,schema.schemaName,"(a int not null, b bigint, c real, d double, e varchar(5))");
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schema)
            .around(tableWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test(expected = SQLException.class)
    public void testCannotInsertNullFieldIntoNonNullColumn() throws Exception {
        runImportTest("null_col.csv",new ErrorCheck() {
            @Override
            public void check(SQLException se) {
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
            public void check(SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!","22001",se.getSQLState());

                String correctErrorMessage = "A truncation error was encountered trying to shrink VARCHAR " +
                        "'thisstringhasmorethanfivecharacters' to length 5.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }

    @Test(expected = SQLException.class)
    public void testCannotInsertALongIntoAnIntegerField() throws Exception {
        runImportTest("long_int.csv",new ErrorCheck() {
            @Override
            public void check(SQLException se) {
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
            public void check(SQLException se) {
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
            public void check(SQLException se) {
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
            public void check(SQLException se) {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", "22003", se.getSQLState());

                String correctErrorMessage = "The resulting value is outside the range for the data type REAL.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage());
            }
        });
    }


    private interface ErrorCheck{
        void check(SQLException se);
    }

    public void runImportTest(String table,ErrorCheck check) throws SQLException {
        String location = getResourceDirectory()+"/test_data/bad_import/"+table;
        PreparedStatement ps = null;
        try{
            ps = methodWatcher.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA(?,?,?,null, ?,',',null,null,null,null)");
            ps.setString(1,schema.schemaName);
            ps.setString(2,TABLE);
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
            check.check(se);

            throw se;
        }
        Assert.fail("No error was returned!");
    }

    public static String getBaseDirectory() {
        String userDir = System.getProperty("user.dir");
        if(!userDir.endsWith("structured_derby"))
            userDir = userDir+"/structured_derby/";
        return userDir;
    }
    public static String getResourceDirectory() {
        return getBaseDirectory()+"/src/test/resources/";
    }
}
