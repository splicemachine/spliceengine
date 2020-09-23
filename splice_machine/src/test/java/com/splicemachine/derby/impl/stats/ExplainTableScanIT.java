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

package com.splicemachine.derby.impl.stats;

import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.math.BigDecimal;
import java.sql.*;

/**
 * Tests around Explain "select stuff from foo" type queries--queries that don't
 * involved indices, joins, or any other stuff. The primary motivation is to
 * ensure that we are looking at the correct statistics information when we
 * perform table scans of specific columns.
 *
 * @author Scott Fines
 *         Date: 4/1/15
 */
public class ExplainTableScanIT extends SpliceUnitTest{
    private static final int size=128;
    private static final SpliceWatcher classWatcher = new SpliceWatcher();
    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(ExplainTableScanIT.class.getSimpleName().toUpperCase());

    private static final String BASE_SCHEMA="a boolean,b smallint,c int,d bigint,e real,f double,g numeric(5,2),h char(5),i varchar(10),l date,m time,n timestamp";
    private static final String INSERTION_SCHEMA="a,b,c,d,e,f,g,h,i,l,m,n";
    private static final SpliceTableWatcher allDataTypes = new SpliceTableWatcher("DT",schema.schemaName,"("+BASE_SCHEMA+")");

    private static final SpliceTableWatcher smallintPk   = new SpliceTableWatcher("smallintPk" ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(b))");
    private static final SpliceTableWatcher intPk        = new SpliceTableWatcher("intPk"      ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(c,b))");
    private static final SpliceTableWatcher bigintPk     = new SpliceTableWatcher("bigintPk"   ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(d,b))");
    private static final SpliceTableWatcher realPk       = new SpliceTableWatcher("realPk"     ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(e,b))");
    private static final SpliceTableWatcher doublePk     = new SpliceTableWatcher("doublePk"   ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(f,b))");
    private static final SpliceTableWatcher numericPk    = new SpliceTableWatcher("numericPk"  ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(g,b))");
    private static final SpliceTableWatcher charPk       = new SpliceTableWatcher("charPk"     ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(h,b))");
    private static final SpliceTableWatcher varcharPk    = new SpliceTableWatcher("varcharPk"  ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(i,b))");
    private static final SpliceTableWatcher datePk       = new SpliceTableWatcher("datePk"     ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(l,b))");
    private static final SpliceTableWatcher timePk       = new SpliceTableWatcher("timePk"     ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(m,b))");
    private static final SpliceTableWatcher timestampPk  = new SpliceTableWatcher("timestampPk",schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(n,b))");

    @ClassRule public static final TestRule rule =RuleChain.outerRule(classWatcher)
            .around(schema)
            .around(allDataTypes)
            .around(smallintPk)
            .around(intPk)
            .around(bigintPk)
            .around(realPk)
            .around(doublePk)
            .around(numericPk)
            .around(charPk)
            .around(varcharPk)
            .around(datePk)
            .around(timePk)
            .around(timestampPk)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try(TestDataBuilder tdBuilder=new TestDataBuilder(schema.schemaName,
                            INSERTION_SCHEMA,
                            classWatcher.getOrCreateConnection(),
                            size/8)){
                        tdBuilder.newTable(allDataTypes.tableName)
                                .newTable(smallintPk.tableName)
                                .newTable(intPk.tableName)
                                .newTable(bigintPk.tableName)
                                .newTable(realPk.tableName)
                                .newTable(doublePk.tableName)
                                .newTable(numericPk.tableName)
                                .newTable(charPk.tableName)
                                .newTable(varcharPk.tableName)
                                .newTable(datePk.tableName)
                                .newTable(timePk.tableName)
                                .newTable(timestampPk.tableName);
                        short bVal=(short)0;
                        int cVal=0;
                        long dVal=0;
                        float eVal=0;
                        double fVal=0;
                        BigDecimal hVal=BigDecimal.ZERO;
                        String iVal=Integer.toString(0);
                        String jVal=Integer.toString(0);
                        Date daVal;
                        Time tVal;
                        Timestamp tsVal;
                        for(int i=0;i<size;i++){
                            daVal=new Date(i);
                            tVal=new Time(i%2);
                            tsVal=new Timestamp(i%4);
                            tdBuilder.booleanField(i%2==0)
                                    .shortField(bVal)
                                    .intField(cVal)
                                    .bigintField(dVal)
                                    .realField(eVal)
                                    .doubleField(fVal)
                                    .numericField(hVal)
                                    .charField(iVal)
                                    .varcharField(jVal)
                                    .dateField(daVal)
                                    .timeField(tVal)
                                    .timestampField(tsVal).rowEnd();

                            bVal++;
                            if(i%2==0) cVal++;
                            if(i%4==0) dVal++;
                            if(i%8==0) eVal+=1.5f;
                            if(i%16==0) fVal+=.75d;
                            if(i%32==0) hVal=hVal.add(BigDecimal.ONE);
                            if(i%64==0) iVal=Integer.toString(Integer.parseInt(iVal)+1);
                            if(i%128==0) jVal=Integer.toString(Integer.parseInt(jVal)+2);
                        }
                        tdBuilder.flush();

                        TestConnection conn=tdBuilder.getConnection();
                        enableAll(allDataTypes.tableName,conn);
                        enableAll(smallintPk.tableName,conn);
                        enableAll(intPk.tableName,conn);
                        enableAll(bigintPk.tableName,conn);
                        enableAll(realPk.tableName,conn);
                        enableAll(doublePk.tableName,conn);
                        enableAll(numericPk.tableName,conn);
                        enableAll(charPk.tableName,conn);
                        enableAll(varcharPk.tableName,conn);
                        enableAll(datePk.tableName,conn);
                        enableAll(timePk.tableName,conn);
                        enableAll(timestampPk.tableName,conn);

                        collect(conn,allDataTypes);
                        collect(conn,smallintPk);
                        collect(conn,intPk);
                        collect(conn,bigintPk);
                        collect(conn,realPk);
                        collect(conn,doublePk);
                        collect(conn,numericPk);
                        collect(conn,charPk);
                        collect(conn,varcharPk);
                        collect(conn,datePk);
                        collect(conn,timePk);
                        collect(conn,timestampPk);

                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            });

    private static void enableAll(String table,TestConnection conn) throws SQLException{
        enable(conn,table,"A");
        enable(conn,table,"B");
        enable(conn,table,"C");
        enable(conn,table,"D");
        enable(conn,table,"E");
        enable(conn,table,"F");
        enable(conn,table,"G");
        enable(conn,table,"H");
        enable(conn,table,"I");
        enable(conn,table,"L");
        enable(conn,table,"M");
        enable(conn,table,"N");
    }


    private static Connection conn;

    @BeforeClass
    public static void setUpClass() throws Exception {
        conn = classWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        ((TestConnection)conn).reset();
    }

    @After
    public void tearDown() throws Exception {
        conn.rollback();
    }

    @Test
    public void noPk_selectall_boolean() throws Exception{
        String query = String.format("explain select * from %s",allDataTypes);
        testCorrectTableScan(query,size);
    }

    @Test
    public void noPk_selecttrue_boolean() throws Exception{
        String query = String.format("explain select * from %s where %s = true",allDataTypes,"a");
        testCorrectTableScan(query,size/2);
    }

    @Test
    public void noPk_selectfalse_boolean() throws Exception{
        String query = String.format("explain select * from %s where %s = false",allDataTypes,"a");
        testCorrectTableScan(query,size/2);
    }

    @Test
    public void noPk_selectall_smallint() throws Exception{
        String query = String.format("explain select * from %s",allDataTypes);
        testCorrectTableScan(query,size);
    }

    @Test
    public void noPk_selectall_int() throws Exception{
        String query = String.format("explain select * from %s",allDataTypes);
        testCorrectTableScan(query,size);
    }

    @Test
    public void noPk_selectall_bigint() throws Exception{
        String query = String.format("explain select * from %s",allDataTypes);
        testCorrectTableScan(query,size);
    }

    @Test
    public void noPk_selectall_real() throws Exception{
        String query = String.format("explain select * from %s",allDataTypes);
        testCorrectTableScan(query,size);
    }

    @Test
    public void noPk_selectall_double() throws Exception{
        String query = String.format("explain select * from %s",allDataTypes);
        testCorrectTableScan(query,size);
    }

    @Test
    public void noPk_selectall_numeric() throws Exception{
        String query = String.format("explain select * from %s",allDataTypes);
        testCorrectTableScan(query,size);
    }

    @Test
    public void noPk_selectall_char() throws Exception{
        String query = String.format("explain select * from %s",allDataTypes);
        testCorrectTableScan(query,size);
    }

    @Test
    public void noPk_selectall_varchar() throws Exception{
        String query = String.format("explain select * from %s",allDataTypes);
        testCorrectTableScan(query,size);
    }

    @Test
    public void noPk_selectall_date() throws Exception{
        String query = String.format("explain select * from %s",allDataTypes);
        testCorrectTableScan(query,size);
    }

    @Test
    public void noPk_selectall_time() throws Exception{
        String query = String.format("explain select * from %s",allDataTypes);
        testCorrectTableScan(query,size);
    }

    @Test
    public void noPk_selectall_timestamp() throws Exception{
        String query = String.format("explain select * from %s",allDataTypes);
        testCorrectTableScan(query,size);
    }

    @Test
    public void pk_selectone_smallint() throws Exception{
        String query = String.format("explain select * from %s where b = 10",smallintPk);
        testCorrectTableScan(query,1);
    }

    @Test
    public void pk_selectone_int() throws Exception{
        String query = String.format("explain select * from %s where c = 10 and b = 10",intPk);
        testCorrectTableScan(query,1);
    }

    @Test
    public void pk_selectone_bigint() throws Exception{
        String query = String.format("explain select * from %s where d = 10 and b = 10",bigintPk);
        testCorrectTableScan(query,1);
    }

    @Test
    public void pk_selectone_real() throws Exception{
        String query = String.format("explain select * from %s where e = 10 and b = 10",realPk);
        testCorrectTableScan(query,1);
    }

    @Test
    public void pk_selectone_double() throws Exception{
        String query = String.format("explain select * from %s where f = 10 and b = 10",doublePk);
        testCorrectTableScan(query,1);
    }

    @Test
    public void pk_selectone_numeric() throws Exception{
        String query = String.format("explain select * from %2$s where g = 10 and b = 10","c",numericPk);
        testCorrectTableScan(query,1);
    }

    @Test
    public void pk_selectone_char() throws Exception{
        String query = String.format("explain select * from %s where h = '10' and b = 10",charPk);
        testCorrectTableScan(query,1);
    }

    @Test
    public void pk_selectone_varchar() throws Exception{
        String query = String.format("explain select * from %s where i = '10' and b = 10",varcharPk);
        testCorrectTableScan(query,1);
    }

    @Test
    public void pk_selectone_date() throws Exception{
        String query = String.format("explain select * from %s where l = DATE('1970-01-01') and b = 10",datePk);
        testCorrectTableScan(query,1);
    }

    @Test
    public void pk_selectone_time() throws Exception{
        String query = String.format("explain select * from %s where m = TIME('00:00:00') and b = 10",timePk);
        testCorrectTableScan(query,1);
    }

    @Test
    public void pk_selectone_timestamp() throws Exception{
        String query = String.format("explain select * from %s where n = TIMESTAMP('1969-12-31 00:00:00') and b = 10",timestampPk);
        testCorrectTableScan(query,1);
    }


    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void testCorrectTableScan(String query, int expectedSize) throws SQLException{
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery(query)){
                Assert.assertTrue("No Rows returned!",rs.next());
                Assert.assertTrue("Missing",rs.next());
                Assert.assertTrue("Missing",rs.next());
                Assert.assertTrue("Incorrect type!",rs.getString(1).contains("TableScan"));
                Assert.assertTrue("Incorrect returned row count!",rs.getString(1).contains("outputRows="+expectedSize));
                //this query is very simple, and should only have one entry --TableScan
                Assert.assertFalse("More than three rows returned!",rs.next());

            }
        }
    }

    private static void enable(Connection conn,String tableName,String columnName) throws SQLException {
        try(CallableStatement enableCall = conn.prepareCall("call SYSCS_UTIL.ENABLE_COLUMN_STATISTICS(?,?,?)")){
            enableCall.setString(1,schema.schemaName);
            enableCall.setString(2,tableName);
            enableCall.setString(3,columnName.toUpperCase());
            enableCall.execute();
        }
    }

    private static void collect(TestConnection conn,SpliceTableWatcher table) throws SQLException{
        try(CallableStatement collectCall = conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,false)")){
            collectCall.setString(1,table.getSchema());
            collectCall.setString(2,table.tableName);
            collectCall.execute();
        }
    }

}
