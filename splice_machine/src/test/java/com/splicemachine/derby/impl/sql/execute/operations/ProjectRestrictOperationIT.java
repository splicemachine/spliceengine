/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.client.am.Types;
import com.splicemachine.db.iapi.reference.Limits;
import splice.com.google.common.base.Joiner;
import splice.com.google.common.collect.Lists;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * This tests basic table scans with and without projection/restriction
 */
public class ProjectRestrictOperationIT extends SpliceUnitTest{
    private static final int MIN_DECIMAL_DIVIDE_SCALE=4;
    private static Logger LOG=Logger.getLogger(ProjectRestrictOperationIT.class);
    public static final String CLASS_NAME=ProjectRestrictOperationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher=new SpliceWatcher(CLASS_NAME);
    public static final String TABLE_NAME="A";
    protected static SpliceSchemaWatcher spliceSchemaWatcher=new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher=new SpliceTableWatcher(TABLE_NAME,CLASS_NAME,"(si varchar(40),sa varchar(40),sc int)");
    protected static SpliceTableWatcher spliceTableWatcher2=new SpliceTableWatcher("B",CLASS_NAME,"(sc int,sd int,se decimal(7,2))");
    protected static SpliceTableWatcher spliceTableWatcher3=new SpliceTableWatcher("d6363",CLASS_NAME,"(a int, b char)");

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher,"test_data/employee.sql",CLASS_NAME))
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try{
                        PreparedStatement ps=spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (?,?,?)",CLASS_NAME,TABLE_NAME));
                        for(int i=0;i<10;i++){
                            ps.setString(1,Integer.toString(i));
                            ps.setString(2,Integer.toString(i)+" Times ten");
                            ps.setInt(3,i);
                            ps.executeUpdate();
                        }

                        ps=spliceClassWatcher.prepareStatement(String.format("insert into %s values (?,?,?)",spliceTableWatcher2.toString()));
                        for(int i=0;i<10;i++){
                            ps.setInt(1,i);
                            ps.setInt(2,i*2+1);
                            ps.setBigDecimal(3,new BigDecimal(i*3));
                            ps.executeUpdate();
                        }
                        spliceClassWatcher.execute(String.format("insert into %s values (1, 'a'), (2, 'b'), (3, 'a'), " +
                                " (4, 'b'), (5, 'a'), (6, 'b')",spliceTableWatcher3.toString()));
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher(CLASS_NAME);

    @Test
    public void testCanDivideSafely() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select sc,sd,se,(sc*sd)/se from "+spliceTableWatcher2.toString()+" where se !=0");
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            int sc=rs.getInt(1);
            int sd=rs.getInt(2);
            BigDecimal se=rs.getBigDecimal(3);
            BigDecimal div=rs.getBigDecimal(4);

            BigDecimal correctDiv=new BigDecimal(sc*sd).divide(se,MIN_DECIMAL_DIVIDE_SCALE,BigDecimal.ROUND_DOWN);
//  SPLICE-898            Assert.assertEquals("Incorrect division!",div.compareTo(correctDiv),0);

            results.add(String.format("sc=%d,sd=%d,se=%f,div=%f",sc,sd,se,div));
        }
        for(String result : results){
            LOG.debug(result);
        }
        Assert.assertEquals("Incorrect row count returned",9,results.size());

    }

    @Test
    public void testResrictWrapperOnTableScanPreparedStatement() throws Exception{
        PreparedStatement s=methodWatcher.prepareStatement(format("select * from %s where si like ?",this.getTableReference(TABLE_NAME)));
        s.setString(1,"%5%");
        ResultSet rs=s.executeQuery();
        int i=0;
        while(rs.next()){
            i++;
            Assert.assertNotNull(rs.getString(1));
            Assert.assertNotNull(rs.getString(2));
        }
        Assert.assertEquals(1,i);
    }

    @Test
    public void testRestrictOnAnythingTableScan() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s %s",this.getTableReference(TABLE_NAME),"where si like '%'"));
        int count=0;
        while(rs.next()){
            Assert.assertNotNull("a.si is null!",rs.getString(1));
            Assert.assertNotNull("b.si is null!",rs.getString(2));
            count++;
        }
        Assert.assertEquals("Incorrect number of rows returned!",10,count);
    }

    @Test
    public void testResrictWrapperOnTableScan() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s %s",this.getTableReference(TABLE_NAME),"where si like '%5%'"));
        int i=0;
        while(rs.next()){
            i++;
            Assert.assertNotNull(rs.getString(1));
            Assert.assertNotNull(rs.getString(2));
        }
        Assert.assertEquals(1,i);
    }

    @Test
    public void testProjectRestrictWrapperOnTableScan() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select si || 'Chicken Dumplings' from %s %s",this.getTableReference(TABLE_NAME),"where si like '%5%'"));
        int i=0;
        while(rs.next()){
            i++;
            Assert.assertNotNull(rs.getString(1));
            Assert.assertEquals("String Concatenation Should Match","5"+"Chicken Dumplings",rs.getString(1));
        }
        Assert.assertEquals(1,i);
    }

    @Test
    public void testInPredicate() throws Exception{
        List<String> corrects=Arrays.asList("1","2","4");
        String correctPredicate=Joiner.on("','").join(corrects);
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where si in ('"+correctPredicate+"')",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            String si=rs.getString(1);
            String sa=rs.getString(2);
            Assert.assertTrue("incorrect si returned!",corrects.contains(si));
            Assert.assertNotNull("incorrect sa!",sa);
            results.add(String.format("si=%s,sa=%s",si,sa));
        }
        Assert.assertEquals("Incorrect num rows returned!",corrects.size(),results.size());
    }

    @Test
    public void testSumOperator() throws Exception{
        int increment=2;
        ResultSet rs=methodWatcher.executeQuery(format("select sc, sc+"+increment+" from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            int sc=rs.getInt(1);
            int scIncr=rs.getInt(2);
            Assert.assertEquals("Sum doesn't work!",sc+increment,scIncr);
            results.add(String.format("sc:%d,scIncr:%d",sc,scIncr));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());
    }

    @Test
    public void testTimesOperator() throws Exception{
        int multiplier=2;
        ResultSet rs=methodWatcher.executeQuery(format("select sc, sc*"+multiplier+" from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            int sc=rs.getInt(1);
            int scIncr=rs.getInt(2);
            Assert.assertEquals("Sum doesn't work!",sc*multiplier,scIncr);
            results.add(String.format("sc:%d,scIncr:%d",sc,scIncr));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());
    }

    @Test
    public void testModulusOperator() throws Exception{
        int modulus=2;
        ResultSet rs=methodWatcher.executeQuery(format("select sc, mod(sc,"+modulus+") from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            int sc=rs.getInt(1);
            int scIncr=rs.getInt(2);
            Assert.assertEquals("Modulus doesn't work!",sc%modulus,scIncr);
            results.add(String.format("sc:%d,scIncr:%d",sc,scIncr));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());
    }

    @Test
    public void testSubStringOperator() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select sa, substr(sa,1,1) from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            String sa=rs.getString(1);
            String sub=rs.getString(2);
            String subSa=sa.substring(0,1);
            Assert.assertEquals("Incorrect sub!",subSa,sub);
            results.add(String.format("sa=%s,sub=%s",sa,sub));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());
    }

    @Test
    public void testConcatOperator() throws Exception{
        String concat="b";
        ResultSet rs=methodWatcher.executeQuery(format("select si,si||'"+concat+"' from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            String sa=rs.getString(1);
            String concatAttempt=rs.getString(2);
            String correct=sa+concat;
            Assert.assertEquals("Incorrect concat!",correct,concatAttempt);
            results.add(String.format("sa=%s,concatAttempt=%s",sa,concatAttempt));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());
    }

    @Test
    public void testUpperLowerOperator() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select sa,UPPER(sa),LOWER(sa) from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            String sa=rs.getString(1);
            String upper=rs.getString(2);
            String lower=rs.getString(3);
            String correctUpper=sa.toUpperCase();
            String correctLower=sa.toLowerCase();
            Assert.assertEquals("upper incorrect",correctUpper,upper);
            Assert.assertEquals("lower incorrect",correctLower,lower);
            results.add(String.format("sa:%s,upper:%s,lower:%s",sa,upper,lower));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());

        rs=methodWatcher.executeQuery(format("select sa,UCASE(sa),LCASE(sa) from %s",this.getTableReference(TABLE_NAME)));
        results=Lists.newArrayList();
        while(rs.next()){
            String sa=rs.getString(1);
            String upper=rs.getString(2);
            String lower=rs.getString(3);
            String correctUpper=sa.toUpperCase();
            String correctLower=sa.toLowerCase();
            Assert.assertEquals("ucase incorrect",correctUpper,upper);
            Assert.assertEquals("lcase incorrect",correctLower,lower);
            results.add(String.format("sa:%s,upper:%s,lower:%s",sa,upper,lower));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());
    }

    @Test
    public void testUpperLowerWithLocale() throws Exception{
        ResultSet rs = methodWatcher.executeQuery("values upper('i', 'en-US')");
        rs.next();
        Assert.assertEquals("upper incorrect with en-US", "I", rs.getString(1));
        rs = methodWatcher.executeQuery("values upper('i', 'tr-TR')");
        rs.next();
        Assert.assertEquals("upper incorrect with tr-TR", "İ", rs.getString(1));
        rs = methodWatcher.executeQuery("values upper('ß', 'de-DE')");
        rs.next();
        Assert.assertEquals("upper incorrect with de-DE", "SS", rs.getString(1));
        rs = methodWatcher.executeQuery("values lower('I', 'en-US')");
        rs.next();
        Assert.assertEquals("lower incorrect with en-US", "i", rs.getString(1));
        rs = methodWatcher.executeQuery("values lower('I', 'tr-TR')");
        rs.next();
        Assert.assertEquals("lower incorrect with tr-TR", "ı", rs.getString(1));

        rs = methodWatcher.executeQuery("values ucase('i', 'en-US')");
        rs.next();
        Assert.assertEquals("upper incorrect with en-US", "I", rs.getString(1));
        rs = methodWatcher.executeQuery("values ucase('i', 'tr-TR')");
        rs.next();
        Assert.assertEquals("upper incorrect with tr-TR", "İ", rs.getString(1));
        rs = methodWatcher.executeQuery("values ucase('ß', 'de-DE')");
        rs.next();
        Assert.assertEquals("upper incorrect with de-DE", "SS", rs.getString(1));
        rs = methodWatcher.executeQuery("values lcase('I', 'en-US')");
        rs.next();
        Assert.assertEquals("lower incorrect with en-US", "i", rs.getString(1));
        rs = methodWatcher.executeQuery("values lcase('I', 'tr-TR')");
        rs.next();
        Assert.assertEquals("lower incorrect with tr-TR", "ı", rs.getString(1));

        try {
            methodWatcher.executeQuery("values lower('I', 'aaa-bbb')");
            Assert.fail("Should fail with invalidate locale");
        } catch (Exception ignored) {
        }

        try {
            methodWatcher.executeQuery("values upper('I', 'aaa-bbb')");
            Assert.fail("Should fail with invalidate locale");
        } catch (Exception ignored) {
        }

        try {
            methodWatcher.executeQuery("values lcase('I', 'aaa-bbb')");
            Assert.fail("Should fail with invalidate locale");
        } catch (Exception ignored) {
        }

        try {
            methodWatcher.executeQuery("values ucase('I', 'aaa-bbb')");
            Assert.fail("Should fail with invalidate locale");
        } catch (Exception ignored) {
        }

        testUpperReturnType(4, Types.CHAR);
        testUpperReturnType(Limits.DB2_CHAR_MAXWIDTH, Types.VARCHAR);
        testUpperReturnType(Limits.DB2_VARCHAR_MAXWIDTH, Types.LONGVARCHAR);
        testUpperReturnType(Limits.DB2_LONGVARCHAR_MAXWIDTH, Types.CLOB);

        rs = methodWatcher.executeQuery("values upper(CURRENT_DATE, 'tr-TR')");
        rs.next();
        Assert.assertEquals("upper return type incorrect", Types.VARCHAR, rs.getMetaData().getColumnType(1));
    }

    private void testUpperReturnType(int width, int expectType) throws Exception {
        String from = "";
        String to = "";
        for (int i = 0; i * 2 <= width; i++) {
           from = from.concat("ß");
           to = to.concat("SS");
        }
        ResultSet rs = methodWatcher.executeQuery(format("values upper('%s', 'de-DE')", from));
        rs.next();
        Assert.assertEquals("upper incorrect with de-DE", to, rs.getString(1));
        Assert.assertEquals("upper return type incorrect", expectType,
                rs.getMetaData().getColumnType(1));
    }

    @Test
    public void testConstantIntCompareFalseFilter() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select * from "+this.getPaddedTableReference("A")+"where 2 < 1");
        Assert.assertFalse("1 or more results were found when none were expected",rs.next());
    }

    @Test
    public void testConstantBooleanFalseFilter() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select * from "+this.getPaddedTableReference("A")+"where false");
        Assert.assertFalse("1 or more results were found when none were expected",rs.next());
    }

    @Test
    public void testArithmeticOperators() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select sc,sd,se, sd-sc,sd+sc,sd*sc,se/sd from "+spliceTableWatcher2+" where true");
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            int sc=rs.getInt(1);
            int sd=rs.getInt(2);
            BigDecimal se=rs.getBigDecimal(3);
            int diff=rs.getInt(4);
            int sum=rs.getInt(5);
            int mult=rs.getInt(6);
            BigDecimal div=rs.getBigDecimal(7);

            int correctDiff=sd-sc;
            int correctSum=sd+sc;
            int correctMult=sd*sc;

            BigDecimal correctDiv=se.divide(new BigDecimal(sd),MIN_DECIMAL_DIVIDE_SCALE,BigDecimal.ROUND_DOWN);

            Assert.assertEquals("Incorrect diff!",correctDiff,diff);
            Assert.assertEquals("Incorrect sum!",correctSum,sum);
            Assert.assertEquals("Incorrect mult!",correctMult,mult);
//  SPLICE-898           Assert.assertEquals("Incorrect Div!",correctDiv.compareTo(div),0);


            results.add(String.format("sc=%d,sd=%d,se=%f,diff=%d,sum=%d,mult=%d,div=%f%n",sc,sd,se,diff,sum,mult,div));
        }

        for(String result : results){
            LOG.info(result);
        }
        Assert.assertEquals("incorrect rows returned",10,results.size());
    }

    @Test
    public void testInWithUncorrelatedSubquery() throws Exception{
        Object[][] expected=new Object[][]{{"Alice"}};

        ResultSet rs=methodWatcher.executeQuery("select t1.empname from staff t1 where t1.empnum in "+
                "(select t3.empnum from works t3 where t3.pnum in "+
                "   (select t2.pnum from proj t2 where t2.city='Tampa'))");
        List results=TestUtils.resultSetToArrays(rs);

        Assert.assertArrayEquals(expected,results.toArray());
    }

    /**
     * Some BOOLEAN expressions used to be transformed to non-equivalent
     * IN lists. Verify that they now return the correct results.
     * Regression test case for DERBY-6363.
     */
    @Test
    public void test_6363() throws Exception {

        assertEquals("A |  2   |  3   |  4   |  5   |  6   |\n" +
                        "---------------------------------------\n" +
                        " 1 |true  |true  |true  |true  |true  |\n" +
                        " 2 |true  |true  |true  |true  |true  |\n" +
                        " 3 |true  |true  |true  |true  |true  |\n" +
                        " 4 |false |false |false |false |false |\n" +
                        " 5 |false |false |false |false |false |\n" +
                        " 6 |false |false |false |false |false |",
                TestUtils.FormattedResult.ResultFactory.toString(methodWatcher.executeQuery( "select a, ((b = 'a' or b = 'b') and a < 4), "
                        + "((b = 'a' or b = 'c' or b = 'b') and a < 4), "
                        + "((b = 'a' or (b = 'c' or b = 'b')) and a < 4), "
                        + "((b = 'a' or b in ('c', 'b')) and a < 4), "
                        + "(a < 4 and (b = 'a' or b = 'b')) "
                        + "from d6363 order by a")));
        assertEquals("A | B | 3 | 4 |\n" +
                        "----------------\n" +
                        " 1 | a | x | y |\n" +
                        " 2 | b | x | y |\n" +
                        " 3 | a | x | y |\n" +
                        " 4 | b | - | - |\n" +
                        " 5 | a | - | - |\n" +
                        " 6 | b | - | - |",
                TestUtils.FormattedResult.ResultFactory.toString(methodWatcher.executeQuery( "select a, b, "
                        + "case when ((b = 'a' or b = 'b') and a < 4) "
                        + "then 'x' else '-' end, "
                        + "case when (a < 4 and (b = 'a' or b = 'b')) "
                        + "then 'y' else '-' end "
                        + "from d6363 order by a")));
    }


}
