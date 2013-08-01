package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Tests indices on compound (multi-key) non-unique indices
 * @author Scott Fines
 *         Created on: 8/1/13
 */
public class CompoundNonUniqueIndexTest {
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static String CLASS_NAME = CompoundNonUniqueIndexTest.class.getSimpleName().toUpperCase();
    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static SpliceTableWatcher twoIndexedColumnsWatcher = new SpliceTableWatcher("A",spliceSchemaWatcher.schemaName,"(a int, b float,c int, d double)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(twoIndexedColumnsWatcher);
    private static final float FLOAT_PRECISION = (float) Math.pow(10, -6);
    private static final float DOUBLE_PRECISION = (float) Math.pow(10, -12);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCanInsertIntoTwoIndexColumns() throws Exception {
        int size = 10;
        //create the index first
        SpliceIndexWatcher a_idx_1 = new SpliceIndexWatcher(twoIndexedColumnsWatcher.tableName, spliceSchemaWatcher.schemaName, "A_IDX_1", spliceSchemaWatcher.schemaName, "(a,b)");
        a_idx_1.starting(null);
        try{
            PreparedStatement ps = methodWatcher.prepareStatement("insert into "+ twoIndexedColumnsWatcher+" (a,b,c,d) values (?,?,?,?)");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ps.setFloat(2,2*i);
                ps.setInt(3,3*i);
                ps.setDouble(4,4*i);
                ps.executeUpdate();
            }

            //validate that we can return every row for every i
            ps = methodWatcher.prepareStatement("select * from "+ twoIndexedColumnsWatcher+" where a = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ResultSet rs = ps.executeQuery();
                try{
                    Assert.assertTrue("No rows returned for query a="+i,rs.next());
                    assertTwoColumnDataCorrect("a="+i,i, rs);

                    //validate that no more rows returned
                    Assert.assertFalse("More than one row returned for query a=" + i, rs.next());
                }finally{
                    rs.close();
                }
            }

            //validate that we don't return anything for bad qualifiers
            for(int i=1;i<size;i++){
                ps.setInt(1,size*i);
                ResultSet rs = ps.executeQuery();
                try{
                    Assert.assertFalse("Rows returned for query a="+(2*i),rs.next());
                }finally{
                    rs.close();
                }
            }

            //validate that we can return every row based on the a and b qualifiers
            ps = methodWatcher.prepareStatement("select * from "+ twoIndexedColumnsWatcher+" where a = ? and b = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ps.setFloat(2,2*i);
                ResultSet rs = ps.executeQuery();
                try{
                    Assert.assertTrue("No rows returned for value a="+i,rs.next());
                    assertTwoColumnDataCorrect("a="+i+",b="+(2*i),i, rs);

                    //validate that no more rows returned
                    Assert.assertFalse("More than one row returned for query a=" + i, rs.next());
                }finally{
                    rs.close();
                }
            }

            //validate that an incorrect qualifier will return no results for two columns
            for(int i=1;i<size;i++){
                //a is good, b is bad
                ps.setInt(1,i);
                ps.setFloat(2,3*i);
                ResultSet rs = ps.executeQuery();
                try{
                    Assert.assertFalse("Rows returned for query a="+i +" and b="+(3*i),rs.next());
                }finally{
                    rs.close();
                }

                //a is bad, b is good
                ps.setInt(1,2*i);
                ps.setFloat(2,2*i);
                rs = ps.executeQuery();
                try{
                    Assert.assertFalse("Rows returned for query a="+(2*i) +" and b="+(2*i),rs.next());
                }finally{
                    rs.close();
                }

                //both a and b are bad
                ps.setInt(1,size*i);
                ps.setFloat(2,3*i);
                rs = ps.executeQuery();
                try{
                    Assert.assertFalse("Rows returned for query a="+(size*i) +" and b="+(3*i),rs.next());
                }finally{
                    rs.close();
                }
            }
        }finally{
            a_idx_1.drop();
        }

    }

    private void assertTwoColumnDataCorrect(String query,int i, ResultSet rs) throws SQLException {
        //validate the correct data returned
        int a = rs.getInt(1);
        Assert.assertFalse("No value for a returned for query a=" + i, rs.wasNull());
        Assert.assertEquals("Incorrect value for a returned for query "+query ,i,a);
        float b = rs.getFloat(2);
        Assert.assertFalse("No value for b returned for query a="+i,rs.wasNull());
        Assert.assertEquals("Incorrect value for b returned for query "+query, 2 * i, b, FLOAT_PRECISION);
        int c = rs.getInt(3);
        Assert.assertFalse("No value for c returned for query a="+i,rs.wasNull());
        Assert.assertEquals("Incorrect value for a returned for query "+query , 3 * i, c);
        double d = rs.getDouble(4);
        Assert.assertFalse("No value for d returned for query a="+i,rs.wasNull());
        Assert.assertEquals("Incorrect value for b returned for query "+query + i, 4 * i, d, DOUBLE_PRECISION);
    }
}
