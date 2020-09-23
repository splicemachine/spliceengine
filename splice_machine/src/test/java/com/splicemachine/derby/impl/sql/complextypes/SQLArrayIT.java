package com.splicemachine.derby.impl.sql.complextypes;


import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLArray;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.*;
import static org.junit.Assert.assertEquals;

/**
 *
 * IT to test SQL Array Handling in the execution stack
 *
 */
public class SQLArrayIT extends SpliceUnitTest {

    private static final SpliceWatcher classWatcher = new SpliceWatcher();
    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(SQLArrayIT.class.getSimpleName().toUpperCase());

    private static final SpliceTableWatcher arrayOne = new SpliceTableWatcher("ARRAY_ONE",schema.schemaName,"(col1 int array)");
    private static final SpliceTableWatcher arrayTwo = new SpliceTableWatcher("ARRAY_TWO",schema.schemaName,"(col1 int array)");
    private static final SpliceTableWatcher arrayThree = new SpliceTableWatcher("ARRAY_THREE",schema.schemaName,"(col1 int array)");
    private static final SpliceTableWatcher arrayFour = new SpliceTableWatcher("ARRAY_FOUR",schema.schemaName,"(col1 int array)");
    private static final SpliceTableWatcher arrayFive = new SpliceTableWatcher("ARRAY_FIVE",schema.schemaName,"(col1 int array)");
    private static final SpliceTableWatcher arraySix = new SpliceTableWatcher("ARRAY_SIX",schema.schemaName,"(col1 int array)");
    private static final SpliceTableWatcher arraySeven = new SpliceTableWatcher("ARRAY_SEVEN",schema.schemaName,"(col1 int array)");
    private static final SpliceTableWatcher arrayEight = new SpliceTableWatcher("ARRAY_EIGHT",schema.schemaName,"(col1 int array)");


    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(schema.schemaName);

    @ClassRule
    public static final TestRule rule = RuleChain.outerRule(classWatcher)
            .around(schema)
            .around(arrayOne)
            .around(arrayTwo)
            .around(arrayThree)
            .around(arrayFour)
            .around(arrayFive)
            .around(arraySix)
            .around(arraySeven)
            .around(arrayEight)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description) {
                    try {
                        classWatcher.executeUpdate(format("insert into %s.%s values ([1,1,1]),(null),([2,1])", schema.schemaName, "ARRAY_TWO"));
                        classWatcher.executeUpdate(format("insert into %s.%s values ([1,1,1]),(null),([2,1])", schema.schemaName, "ARRAY_THREE"));
                        classWatcher.executeUpdate(format("insert into %s.%s values ([1,1,1]),(null),([2,1])", schema.schemaName, "ARRAY_FOUR"));
                        classWatcher.executeUpdate(format("insert into %s.%s values ([1,1,1]),(null),([2,1])", schema.schemaName, "ARRAY_FIVE"));
                        classWatcher.executeUpdate(format("insert into %s.%s values ([1,1,1]),(null),([2,1])", schema.schemaName, "ARRAY_SIX"));
                        classWatcher.executeUpdate(format("insert into %s.%s values ([1,1,1]),(null),([2,1])", schema.schemaName, "ARRAY_SEVEN"));
                        classWatcher.executeUpdate(format("insert into %s.%s values ([1,1,1]),([1,3]),([2,1]),([2,2]),([3])", schema.schemaName, "ARRAY_EIGHT"));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

            });

    @Test
    public void testStatistics() throws Exception {
        methodWatcher.executeQuery("analyze schema " + schema.schemaName);
    }

    @Test
    public void testInsertSelectValues() throws Exception {
        methodWatcher.executeUpdate("insert into ARRAY_ONE values ([1,1,1]),(null),([2,1])");
        ResultSet rs = methodWatcher.executeQuery("select cast(col1  as varchar(30)), col1[0], col1[1], col1[2] from ARRAY_ONE");
        assertEquals("1     |  2  |  3  |  4  |\n" +
                "-----------------------------\n" +
                "  NULL    |NULL |NULL |NULL |\n" +
                "[1, 1, 1] |  1  |  1  |  1  |\n" +
                " [2, 1]   |  2  |  1  |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testQualifier() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from ARRAY_TWO where col1 = [2,1]");
        assertEquals("COL1  |\n" +
                "--------\n" +
                "[2, 1] |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testParameterQualifier() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement("select * from ARRAY_TWO where col1 = ?");
        ps.setArray(1,new SQLArray(new DataValueDescriptor[]{new SQLInteger(2),new SQLInteger(1)}));
        ResultSet rs = ps.executeQuery();
        assertEquals("COL1  |\n" +
                "--------\n" +
                "[2, 1] |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testOperatorQualifier() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from ARRAY_TWO where col1[1] = 1");
        assertEquals("COL1    |\n" +
                "-----------\n" +
                "[1, 1, 1] |\n" +
                " [2, 1]   |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testAggregation() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select sum(col1[1]) from ARRAY_TWO where col1[1] = 1");
        assertEquals("1 |\n" +
                "----\n" +
                " 2 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testUpdate() throws Exception {
        methodWatcher.executeUpdate("update ARRAY_THREE set col1 = [5,4,3] where col1 = [2,1]");
        ResultSet rs = methodWatcher.executeQuery("select col1 from ARRAY_THREE where col1 = [5,4,3]");
        assertEquals("COL1    |\n" +
                "-----------\n" +
                "[5, 4, 3] |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testParameterUpdate() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement("update ARRAY_THREE set col1=? where col1 = ?");
        ps.setArray(1,new SQLArray(new DataValueDescriptor[]{new SQLInteger(100),new SQLInteger(100)}));
        ps.setArray(2,new SQLArray(new DataValueDescriptor[]{new SQLInteger(1),new SQLInteger(1),new SQLInteger(1)}));
        int rows = ps.executeUpdate();
        assertEquals("row not updated properly",1,rows);
        PreparedStatement ps2 = methodWatcher.prepareStatement("select col1 from ARRAY_THREE where col1 = ?");
        ps2.setArray(1,new SQLArray(new DataValueDescriptor[]{new SQLInteger(100),new SQLInteger(100)}));
        ResultSet rs2 = ps2.executeQuery();
        assertEquals("COL1    |\n" +
                "------------\n" +
                "[100, 100] |", TestUtils.FormattedResult.ResultFactory.toString(rs2));
    }

    @Test
    public void testDelete() throws Exception {
        methodWatcher.executeUpdate("delete from ARRAY_FOUR where col1 = [2,1]");
        ResultSet rs = methodWatcher.executeQuery("select col1 from ARRAY_FOUR where col1 = [2,1]");
        assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testParameterDelete() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement("delete from ARRAY_FOUR where col1 = ?");
        ps.setArray(1,new SQLArray(new DataValueDescriptor[]{new SQLInteger(1),new SQLInteger(1),new SQLInteger(1)}));
        int rows = ps.executeUpdate();
        assertEquals("row not delete properly",1,rows);
        ResultSet rs = methodWatcher.executeQuery("select col1 from ARRAY_FOUR where col1 = [1,1,1]");
        assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testImportArrays() {

    }

    @Test
    public void testExportArrays() {

    }

    @Test
    public void testJoinOnArrayPosition() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from array_five inner join array_six " +
                "on array_five.col1[0] = array_six.col1[0]");
        assertEquals("COL1    |  COL1    |\n" +
                "----------------------\n" +
                "[1, 1, 1] |[1, 1, 1] |\n" +
                " [2, 1]   | [2, 1]   |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testJoinOnArray() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from array_five inner join array_six " +
                "on array_five.col1 = array_six.col1");
        assertEquals("COL1    |  COL1    |\n" +
                "----------------------\n" +
                "[1, 1, 1] |[1, 1, 1] |\n" +
                " [2, 1]   | [2, 1]   |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testSortOnArray() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from array_two order by col1 nulls first");
        assertEquals("COL1    |\n" +
                "-----------\n" +
                "  NULL    |\n" +
                "[1, 1, 1] |\n" +
                " [2, 1]   |", TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));

        ResultSet rs2 = methodWatcher.executeQuery("select * from array_two order by col1 nulls last");
        assertEquals("COL1    |\n" +
                "-----------\n" +
                "[1, 1, 1] |\n" +
                " [2, 1]   |\n" +
                "  NULL    |", TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs2));

    }

    @Test
    public void testSortOnArrayElement() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select col1[0] from array_two order by col1[0] nulls first");
        assertEquals("1  |\n" +
                "------\n" +
                "NULL |\n" +
                "  1  |\n" +
                "  2  |", TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));

        ResultSet rs2 = methodWatcher.executeQuery("select col1[0] from array_two order by col1[0] nulls last");
        assertEquals("1  |\n" +
                "------\n" +
                "  1  |\n" +
                "  2  |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs2));
    }

    @Test
    public void testGroupByOnArrayElement() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select col1[0], max(col1[1]) from array_eight group by col1[0]");
        assertEquals(
                "1 |  2  |\n" +
                "----------\n" +
                " 1 |  3  |\n" +
                " 2 |  2  |\n" +
                " 3 |NULL |", TestUtils.FormattedResult.ResultFactory.toString(rs));

    }

    @Test
    public void testPrimaryKey() throws Exception {
        methodWatcher.executeUpdate("create table foo (col1 int array, primary key(col1))");
        methodWatcher.executeUpdate("insert into foo values ([1,1,1])");
        ResultSet rs = methodWatcher.executeQuery("select * from foo");
        assertEquals("COL1    |\n" +
                "-----------\n" +
                "[1, 1, 1] |", TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
    }

    @Test
    public void testArrayOperatorNodeExplain() throws Exception {
        String query = "explain select * from array_one where col1[1] = 1";
        this.rowContainsQuery(3,query,"preds=[(COL1[0:1][1] = 1)]",methodWatcher);
    }

    @Test
    public void testArrayConstantNodeExplain() throws Exception {
        String query = "explain select * from array_one where col1 = [1,1,1]";
        this.rowContainsQuery(3,query,"preds=[(COL1[0:1] = [1,1,1])]",methodWatcher);
    }

    @Test
    public void testRI() {
        // TODO, should just work
    }

}