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

    private static SpliceTableWatcher twoContiguousColumns = new SpliceTableWatcher("TWO_CONTIGUOUS",spliceSchemaWatcher.schemaName,"(a int, b float,c int, d double)");
    private static SpliceTableWatcher twoContiguousAscDescColumns = new SpliceTableWatcher("TWO_CONTIGUOUS_ASC_DESC",spliceSchemaWatcher.schemaName,"(a int, b float,c int, d double)");
    private static SpliceTableWatcher twoContiguousDescAscColumns = new SpliceTableWatcher("TWO_CONTIGUOUS_DESC_ASC",spliceSchemaWatcher.schemaName,"(a int, b float,c int, d double)");
    private static SpliceTableWatcher twoNonContiguousColumns = new SpliceTableWatcher("TWO_NONCONTIGUOUS",spliceSchemaWatcher.schemaName,"(a int, b float,c int, d double)");
    private static SpliceTableWatcher twoOutOfOrderNonContiguousColumns = new SpliceTableWatcher("TWO_NONCONTIGUOUS_OUT_OF_ORDER",spliceSchemaWatcher.schemaName,"(a int, b float,c int, d double)");
    private static SpliceTableWatcher threeNonContiguousColumns = new SpliceTableWatcher("THREE_NONCONTIGUOUS",spliceSchemaWatcher.schemaName,"(a int, b float,c int, d double)");
    private static SpliceTableWatcher threeOutOfOrderNonContiguousColumns = new SpliceTableWatcher("TTHREE_NONCONTIGUOUS_OUT_OF_ORDER",spliceSchemaWatcher.schemaName,"(a int, b float,c int, d double)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(twoContiguousColumns)
            .around(twoContiguousAscDescColumns)
            .around(twoContiguousDescAscColumns)
            .around(twoNonContiguousColumns)
            .around(twoOutOfOrderNonContiguousColumns)
            .around(threeOutOfOrderNonContiguousColumns)
            .around(threeNonContiguousColumns);
    private static final float FLOAT_PRECISION = (float) Math.pow(10, -6);
    private static final float DOUBLE_PRECISION = (float) Math.pow(10, -12);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCanInsertIntoThreeNonContiguousColumns() throws Exception {
        int size = 10;
        //create the index first
        SpliceIndexWatcher a_idx_1 = new SpliceIndexWatcher(threeNonContiguousColumns.tableName, spliceSchemaWatcher.schemaName, "THREE_IDX_1", spliceSchemaWatcher.schemaName, "(a,c,d)");
        a_idx_1.starting(null);
        try{
            PreparedStatement ps = methodWatcher.prepareStatement("insert into "+ threeNonContiguousColumns +" (a,b,c,d) values (?,?,?,?)");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ps.setFloat(2,2*i);
                ps.setInt(3,3*i);
                ps.setDouble(4,4*i);
                ps.executeUpdate();
            }

            //check that all values of a return correctly
            ps = methodWatcher.prepareStatement("select * from "+ threeNonContiguousColumns +" where a = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ResultSet rs = ps.executeQuery();
                assertColumnDataCorrect("a = "+ i,i,rs,true);


                if(i==0) continue;
                //check bad a
                ps.setInt(1,size*i);
                rs = ps.executeQuery();
                assertColumnDataCorrect("a = "+ (size*i),i,rs,false);
            }


            ps = methodWatcher.prepareStatement("select * from "+ threeNonContiguousColumns +" where c = ?");
            for(int i=0;i<size;i++){

                //check that all values of c return correctly
                ps.setInt(1,3*i);
                ResultSet rs = ps.executeQuery();
                assertColumnDataCorrect("c = "+(3*i),i,rs,true);

                if(i==0) continue;
                //check bad values for c
                ps.setInt(1,(size+1)*i);
                rs = ps.executeQuery();
                assertColumnDataCorrect("c = "+ ((size+1)*i),i,rs,false);
            }

            ps = methodWatcher.prepareStatement("select * from "+ threeNonContiguousColumns +" where d = ?");
            for(int i=0;i<size;i++){
                //check that all values of d return correctly
                ps.setDouble(1,4*i);
                ResultSet rs = ps.executeQuery();
                assertColumnDataCorrect("d = "+ (4*i),i,rs,true);

                if(i==0) continue;
                //check bad values for d
                ps.setDouble(1,(size+1)*i);
                rs = ps.executeQuery();
                assertColumnDataCorrect("d = "+ ((size+1)*i),i,rs,false);
            }


            //check that all values of a=? and c = ? work
            ps = methodWatcher.prepareStatement("select * from "+ threeNonContiguousColumns +" where a = ? and c = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ps.setInt(2,3*i);
                assertColumnDataCorrect("a = "+ i+" and c = "+(3*i),i,ps.executeQuery(),true);

                if(i==0) continue;
                //check bad values for a, good for c
                int a = (size*i);
                int c = 3*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                assertColumnDataCorrect("a = "+ (size*i)+" and c = "+(3*i),i,ps.executeQuery(),false);

                //check bad values for c, good for a
                a = i;
                c = ((size+1)*i);
                ps.setInt(1,a);
                ps.setInt(2,c);
                assertColumnDataCorrect("a = "+ a+" and c = "+c,i,ps.executeQuery(),false);

                //check bad values for a and c
                a = (size*i);
                c = ((size+1)*i);
                ps.setInt(1,a);
                ps.setInt(2,c);
                assertColumnDataCorrect("a = "+ a+" and c = "+c,i,ps.executeQuery(),false);
            }


            //check that all values of a=? and d = ? work
            ps = methodWatcher.prepareStatement("select * from "+ threeNonContiguousColumns +" where a = ? and d = ?");
            for(int i=0;i<size;i++){
                int a = i;
                double d = 4*i;
                ps.setInt(1,a);
                ps.setDouble(2,d);
                assertColumnDataCorrect("a = "+ a+" and d = "+d,i,ps.executeQuery(),true);

                if(i==0)continue;

                //check bad values for a, good for d
                a = (size*i);
                d = 4*i;
                ps.setInt(1,a);
                ps.setDouble(2,d);
                assertColumnDataCorrect("a = "+ a+" and d = "+d,i,ps.executeQuery(),false);

                //check bad values for d, good for a
                a = i;
                d = (size+1)*i;
                ps.setInt(1,a);
                ps.setDouble(2,d);
                assertColumnDataCorrect("a = "+ a+" and d = "+d,i,ps.executeQuery(),false);

                //check bad values for a and d
                a = (size*i);
                d = (size+1)*i;
                ps.setInt(1,a);
                ps.setDouble(2,d);
                assertColumnDataCorrect("a = "+ a+" and d = "+d,i,ps.executeQuery(),false);
            }


            ps = methodWatcher.prepareStatement("select * from "+ threeNonContiguousColumns +" where a = ? and c = ? and d = ?");
            for(int i=0;i<size;i++){
                //check good values for a,c and d
                int a = i;
                int c = 3*i;
                double d = 4*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),true);

                if(i==0) continue;
                //check bad values for a, good for c and d
                a = (size*i);
                c = 3*i;
                d = 4*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);

                //check bad c,good a and d
                a = i;
                c = (size+1)*i;
                d = 4*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);

                //check bad d,good a and c
                a = i;
                c = (3)*i;
                d = (size+1)*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);

                //check bad a,c, good d
                a = (size*i);
                c = (size+1)*i;
                d = (4)*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);

                //check bad a,d, good c
                a = (size*i);
                c = (3)*i;
                d = (size+1)*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);

                //check bad c,d, good a
                a = (i);
                c = (size+1)*i;
                d = (size+1)*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);

                //check bad c,d, a
                a = (size*i);
                c = (size+1)*i;
                d = (size+1)*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);
            }
        }finally{
            a_idx_1.drop();
        }
    }

    @Test
    public void testCanInsertIntoTwoIndexColumns() throws Exception {
        int size = 10;
        //create the index first
        SpliceIndexWatcher a_idx_1 = new SpliceIndexWatcher(twoContiguousColumns.tableName, spliceSchemaWatcher.schemaName, "TWO_IDX_1", spliceSchemaWatcher.schemaName, "(a,b)");
        a_idx_1.starting(null);
        try{
            PreparedStatement ps = methodWatcher.prepareStatement("insert into "+ twoContiguousColumns +" (a,b,c,d) values (?,?,?,?)");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ps.setFloat(2,2*i);
                ps.setInt(3,3*i);
                ps.setDouble(4,4*i);
                ps.executeUpdate();
            }

            //validate that we can return every row for every i
            ps = methodWatcher.prepareStatement("select * from "+ twoContiguousColumns +" where a = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+i,i, rs,true);
                }finally{
                    rs.close();
                }
            }

            //validate that we don't return anything for bad qualifiers
            for(int i=1;i<size;i++){
                ps.setInt(1,size*i);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+(size*i),i,rs,false);
                }finally{
                    rs.close();
                }
            }

            //validate that we can return every row based on the a and b qualifiers
            ps = methodWatcher.prepareStatement("select * from "+ twoContiguousColumns +" where a = ? and b = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ps.setFloat(2,2*i);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+i+",b="+(2*i),i,rs,true);
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
                    assertColumnDataCorrect("a="+i+"and b="+(3*i),i,rs,false);
                }finally{
                    rs.close();
                }

                //a is bad, b is good
                ps.setInt(1,2*i);
                ps.setFloat(2,2*i);
                rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+(2*i)+"and b="+(2*i),i,rs,false);
//                    Assert.assertFalse("Rows returned for query a="+(2*i) +" and b="+(2*i),rs.next());
                }finally{
                    rs.close();
                }

                //both a and b are bad
                ps.setInt(1,size*i);
                ps.setFloat(2,3*i);
                rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+(size*i)+"and b="+(3*i),i,rs,false);
                }finally{
                    rs.close();
                }
            }
        }finally{
            a_idx_1.drop();
        }

    }

    @Test
    public void testCanInsertIntoTwoIndexColumnsAscDesc() throws Exception {
        int size = 10;
        //create the index first
        SpliceIndexWatcher a_idx_1 = new SpliceIndexWatcher(twoContiguousAscDescColumns.tableName, spliceSchemaWatcher.schemaName, "TWO_IDX_1", spliceSchemaWatcher.schemaName, "(a asc,b desc)");
        a_idx_1.starting(null);
        try{
            PreparedStatement ps = methodWatcher.prepareStatement("insert into "+ twoContiguousAscDescColumns +" (a,b,c,d) values (?,?,?,?)");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ps.setFloat(2,2*i);
                ps.setInt(3,3*i);
                ps.setDouble(4,4*i);
                ps.executeUpdate();
            }

            //validate that we can return every row for every i
            ps = methodWatcher.prepareStatement("select * from "+ twoContiguousAscDescColumns +" where a = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+i,i, rs,true);
                }finally{
                    rs.close();
                }
            }

            //validate that we don't return anything for bad qualifiers
            for(int i=1;i<size;i++){
                ps.setInt(1,size*i);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+(size*i),i,rs,false);
                }finally{
                    rs.close();
                }
            }

            //validate that we can return every row based on the a and b qualifiers
            ps = methodWatcher.prepareStatement("select * from "+ twoContiguousAscDescColumns +" where a = ? and b = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ps.setFloat(2,2*i);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+i+",b="+(2*i),i,rs,true);
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
                    assertColumnDataCorrect("a="+i+"and b="+(3*i),i,rs,false);
                }finally{
                    rs.close();
                }

                //a is bad, b is good
                ps.setInt(1,2*i);
                ps.setFloat(2,2*i);
                rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+(2*i)+"and b="+(2*i),i,rs,false);
                }finally{
                    rs.close();
                }

                //both a and b are bad
                ps.setInt(1,size*i);
                ps.setFloat(2,3*i);
                rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+(size*i)+"and b="+(3*i),i,rs,false);
                }finally{
                    rs.close();
                }
            }
        }finally{
            a_idx_1.drop();
        }

    }
    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumns() throws Exception {
        int size = 10;
        //create the index first
        SpliceIndexWatcher a_idx_1 = new SpliceIndexWatcher(twoContiguousColumns.tableName, spliceSchemaWatcher.schemaName, "TWO_IDX_2", spliceSchemaWatcher.schemaName, "(a,c)");
        a_idx_1.starting(null);
        try{
            PreparedStatement ps = methodWatcher.prepareStatement("insert into "+ twoNonContiguousColumns +" (a,b,c,d) values (?,?,?,?)");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ps.setFloat(2,2*i);
                ps.setInt(3,3*i);
                ps.setDouble(4,4*i);
                ps.executeUpdate();
            }

            //validate that we can return every row for every i
            ps = methodWatcher.prepareStatement("select * from "+ twoNonContiguousColumns +" where a = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+i,i, rs,true);
                }finally{
                    rs.close();
                }
            }

            //validate that we don't return anything for bad qualifiers
            for(int i=1;i<size;i++){
                ps.setInt(1,size*i);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+(size*i),i,rs,false);
                }finally{
                    rs.close();
                }
            }

            //validate that we can return every row based on the a and b qualifiers
            ps = methodWatcher.prepareStatement("select * from "+ twoNonContiguousColumns +" where a = ? and c = ?");
            for(int i=0;i<size;i++){
                int a = i;
                int c = (3*i);
                ps.setInt(1,a);
                ps.setInt(2,c);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+a+",c="+c,i,rs,true);
                }finally{
                    rs.close();
                }
            }

            //validate that an incorrect qualifier will return no results for two columns
            for(int i=1;i<size;i++){
                //bad a, good c
                int a = (size*i);
                int c = (3*i);
                ps.setInt(1,a);
                ps.setInt(2,c);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+a+",c="+c,i,rs,false);
                }finally{
                    rs.close();
                }

                //bad c, good a
                a = (i);
                c = (2*i);
                ps.setInt(1,a);
                ps.setInt(2,c);
                rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+a+",c="+c,i,rs,false);
                }finally{
                    rs.close();
                }

                //bad a,c
                a = (size*i);
                c = (2*i);
                ps.setInt(1,a);
                ps.setInt(2,c);
                rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+a+",c="+c,i,rs,false);
                }finally{
                    rs.close();
                }
            }
        }finally{
            a_idx_1.drop();
        }

    }

    @Test
    public void testCanInsertIntoTwoNonContiguousIndexColumnsOutOfOrder() throws Exception {
        int size = 10;
        //create the index first
        SpliceIndexWatcher a_idx_1 = new SpliceIndexWatcher(twoOutOfOrderNonContiguousColumns.tableName, spliceSchemaWatcher.schemaName, "TWO_IDX_3", spliceSchemaWatcher.schemaName, "(c,a)");
        a_idx_1.starting(null);
        try{
            PreparedStatement ps = methodWatcher.prepareStatement("insert into "+ twoOutOfOrderNonContiguousColumns +" (a,b,c,d) values (?,?,?,?)");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ps.setFloat(2,2*i);
                ps.setInt(3,3*i);
                ps.setDouble(4,4*i);
                ps.executeUpdate();
            }

            //validate that we can return every row for every i
            ps = methodWatcher.prepareStatement("select * from "+ twoOutOfOrderNonContiguousColumns +" where a = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+i,i, rs,true);
                }finally{
                    rs.close();
                }
            }

            //validate that we don't return anything for bad qualifiers
            for(int i=1;i<size;i++){
                ps.setInt(1,size*i);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+(size*i),i,rs,false);
                }finally{
                    rs.close();
                }
            }

            ps = methodWatcher.prepareStatement("select * from "+ twoOutOfOrderNonContiguousColumns +" where c = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,3*i);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("c="+3*i,i, rs,true);
                }finally{
                    rs.close();
                }
            }

            //validate that we don't return anything for bad qualifiers
            for(int i=1;i<size;i++){
                ps.setInt(1,(size+1)*i);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("c="+((size+1)*i),i,rs,false);
                }finally{
                    rs.close();
                }
            }

            //validate that we can return every row based on the a and b qualifiers
            ps = methodWatcher.prepareStatement("select * from "+ twoOutOfOrderNonContiguousColumns +" where a = ? and c = ?");
            for(int i=0;i<size;i++){
                int a = i;
                int c = (3*i);
                ps.setInt(1,a);
                ps.setInt(2,c);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+a+",c="+c,i,rs,true);
                }finally{
                    rs.close();
                }
            }

            //validate that an incorrect qualifier will return no results for two columns
            for(int i=1;i<size;i++){
                //bad a, good c
                int a = (size*i);
                int c = (3*i);
                ps.setInt(1,a);
                ps.setInt(2,c);
                ResultSet rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+a+",c="+c,i,rs,false);
                }finally{
                    rs.close();
                }

                //bad c, good a
                a = (i);
                c = (2*i);
                ps.setInt(1,a);
                ps.setInt(2,c);
                rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+a+",c="+c,i,rs,false);
                }finally{
                    rs.close();
                }

                //bad a,c
                a = (size*i);
                c = (2*i);
                ps.setInt(1,a);
                ps.setInt(2,c);
                rs = ps.executeQuery();
                try{
                    assertColumnDataCorrect("a="+a+",c="+c,i,rs,false);
                }finally{
                    rs.close();
                }
            }
        }finally{
            a_idx_1.drop();
        }

    }

    @Test
    public void testCanInsertIntoThreeNonContiguousColumnsOutOfOrder() throws Exception {
        int size = 10;
        //create the index first
        SpliceIndexWatcher a_idx_1 = new SpliceIndexWatcher(threeOutOfOrderNonContiguousColumns.tableName, spliceSchemaWatcher.schemaName, "THREE_IDX_1", spliceSchemaWatcher.schemaName, "(a,c,d)");
        a_idx_1.starting(null);
        try{
            PreparedStatement ps = methodWatcher.prepareStatement("insert into "+ threeOutOfOrderNonContiguousColumns +" (a,b,c,d) values (?,?,?,?)");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ps.setFloat(2,2*i);
                ps.setInt(3,3*i);
                ps.setDouble(4,4*i);
                ps.executeUpdate();
            }

            //check that all values of a return correctly
            ps = methodWatcher.prepareStatement("select * from "+ threeOutOfOrderNonContiguousColumns +" where a = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ResultSet rs = ps.executeQuery();
                assertColumnDataCorrect("a = "+ i,i,rs,true);


                if(i==0) continue;
                //check bad a
                ps.setInt(1,size*i);
                rs = ps.executeQuery();
                assertColumnDataCorrect("a = "+ (size*i),i,rs,false);
            }


            ps = methodWatcher.prepareStatement("select * from "+ threeOutOfOrderNonContiguousColumns+" where c = ?");
            for(int i=0;i<size;i++){

                //check that all values of c return correctly
                ps.setInt(1,3*i);
                ResultSet rs = ps.executeQuery();
                assertColumnDataCorrect("c = "+(3*i),i,rs,true);

                if(i==0) continue;
                //check bad values for c
                ps.setInt(1,(size+1)*i);
                rs = ps.executeQuery();
                assertColumnDataCorrect("c = "+ ((size+1)*i),i,rs,false);
            }

            ps = methodWatcher.prepareStatement("select * from "+ threeOutOfOrderNonContiguousColumns+" where d = ?");
            for(int i=0;i<size;i++){
                //check that all values of d return correctly
                ps.setDouble(1,4*i);
                ResultSet rs = ps.executeQuery();
                assertColumnDataCorrect("d = "+ (4*i),i,rs,true);

                if(i==0) continue;
                //check bad values for d
                ps.setDouble(1,(size+1)*i);
                rs = ps.executeQuery();
                assertColumnDataCorrect("d = "+ ((size+1)*i),i,rs,false);
            }


            //check that all values of a=? and c = ? work
            ps = methodWatcher.prepareStatement("select * from "+ threeOutOfOrderNonContiguousColumns+" where a = ? and c = ?");
            for(int i=0;i<size;i++){
                ps.setInt(1,i);
                ps.setInt(2,3*i);
                assertColumnDataCorrect("a = "+ i+" and c = "+(3*i),i,ps.executeQuery(),true);

                if(i==0) continue;
                //check bad values for a, good for c
                int a = (size*i);
                int c = 3*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                assertColumnDataCorrect("a = "+ (size*i)+" and c = "+(3*i),i,ps.executeQuery(),false);

                //check bad values for c, good for a
                a = i;
                c = ((size+1)*i);
                ps.setInt(1,a);
                ps.setInt(2,c);
                assertColumnDataCorrect("a = "+ a+" and c = "+c,i,ps.executeQuery(),false);

                //check bad values for a and c
                a = (size*i);
                c = ((size+1)*i);
                ps.setInt(1,a);
                ps.setInt(2,c);
                assertColumnDataCorrect("a = "+ a+" and c = "+c,i,ps.executeQuery(),false);
            }


            //check that all values of a=? and d = ? work
            ps = methodWatcher.prepareStatement("select * from "+ threeOutOfOrderNonContiguousColumns+" where a = ? and d = ?");
            for(int i=0;i<size;i++){
                int a = i;
                double d = 4*i;
                ps.setInt(1,a);
                ps.setDouble(2,d);
                assertColumnDataCorrect("a = "+ a+" and d = "+d,i,ps.executeQuery(),true);

                if(i==0)continue;

                //check bad values for a, good for d
                a = (size*i);
                d = 4*i;
                ps.setInt(1,a);
                ps.setDouble(2,d);
                assertColumnDataCorrect("a = "+ a+" and d = "+d,i,ps.executeQuery(),false);

                //check bad values for d, good for a
                a = i;
                d = (size+1)*i;
                ps.setInt(1,a);
                ps.setDouble(2,d);
                assertColumnDataCorrect("a = "+ a+" and d = "+d,i,ps.executeQuery(),false);

                //check bad values for a and d
                a = (size*i);
                d = (size+1)*i;
                ps.setInt(1,a);
                ps.setDouble(2,d);
                assertColumnDataCorrect("a = "+ a+" and d = "+d,i,ps.executeQuery(),false);
            }


            ps = methodWatcher.prepareStatement("select * from "+ threeOutOfOrderNonContiguousColumns +" where a = ? and c = ? and d = ?");
            for(int i=0;i<size;i++){
                //check good values for a,c and d
                int a = i;
                int c = 3*i;
                double d = 4*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),true);

                if(i==0) continue;
                //check bad values for a, good for c and d
                a = (size*i);
                c = 3*i;
                d = 4*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);

                //check bad c,good a and d
                a = i;
                c = (size+1)*i;
                d = 4*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);

                //check bad d,good a and c
                a = i;
                c = (3)*i;
                d = (size+1)*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);

                //check bad a,c, good d
                a = (size*i);
                c = (size+1)*i;
                d = (4)*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);

                //check bad a,d, good c
                a = (size*i);
                c = (3)*i;
                d = (size+1)*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);

                //check bad c,d, good a
                a = (i);
                c = (size+1)*i;
                d = (size+1)*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);

                //check bad c,d, a
                a = (size*i);
                c = (size+1)*i;
                d = (size+1)*i;
                ps.setInt(1,a);
                ps.setInt(2,c);
                ps.setDouble(3,d);
                assertColumnDataCorrect("a = "+ a+" and c = "+ c+ " and d = "+d,i,ps.executeQuery(),false);
            }
        }finally{
            a_idx_1.drop();
        }
    }

    private void assertColumnDataCorrect(String query,int i, ResultSet rs,boolean expectData) throws SQLException {
        try{
            if(!expectData){
                Assert.assertFalse("Rows returned for query "+ query,rs.next());
            }else{
                Assert.assertTrue("No Rows returned for query "+ query,rs.next());

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

                Assert.assertFalse("Too many Rows returned for query "+ query,rs.next());
            }
        }finally{
            rs.close();
        }
    }

}
