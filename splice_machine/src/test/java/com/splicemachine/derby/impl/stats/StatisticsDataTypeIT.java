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

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.SlowTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.math.BigDecimal;
import java.sql.*;

/**
 * Tests for data type correctness w.r.t different column types
 *
 * @author Scott Fines
 *         Date: 3/3/15
 */
@Category(SlowTest.class)
public class StatisticsDataTypeIT {
    private static final int size=128;
    private static final SpliceWatcher classWatcher = new SpliceWatcher();
    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(StatisticsDataTypeIT.class.getSimpleName().toUpperCase());

    private static final String BASE_SCHEMA="a boolean,b smallint,c int,d bigint,e real,f double,g numeric(5,2),h char(5),i varchar(10),j blob,k clob,l date,m time,n timestamp";
    private static final String INSERTION_SCHEMA="a,b,c,d,e,f,g,h,i,l,m,n";
    private static final SpliceTableWatcher allDataTypes            = new SpliceTableWatcher("DT"               ,schema.schemaName,"("+BASE_SCHEMA+")");
    private static final SpliceTableWatcher smallintPk              = new SpliceTableWatcher("smallintPk"       ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(a,b))");
    private static final SpliceTableWatcher intPk                   = new SpliceTableWatcher("intPk"            ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(a,b,c))");
    private static final SpliceTableWatcher bigintPk                = new SpliceTableWatcher("bigintPk"         ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(a,b,c,d))");
    private static final SpliceTableWatcher realPk                  = new SpliceTableWatcher("realPk"           ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(a,b,c,d,e))");
    private static final SpliceTableWatcher doublePk                = new SpliceTableWatcher("doublePk"         ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(a,b,c,d,e,f))");
    private static final SpliceTableWatcher numericPk               = new SpliceTableWatcher("numericPk"        ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(a,b,c,d,e,f,g))");
    private static final SpliceTableWatcher charPk                  = new SpliceTableWatcher("charPk"           ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(a,b,c,d,e,f,g,h))");
    private static final SpliceTableWatcher varcharPk               = new SpliceTableWatcher("varcharPk"        ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(a,b,c,d,e,f,g,h,i))");
    private static final SpliceTableWatcher datePk                  = new SpliceTableWatcher("datePk"           ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(a,b,c,d,e,f,g,h,i,l))");
    private static final SpliceTableWatcher timePk                  = new SpliceTableWatcher("timePk"           ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(a,b,c,d,e,f,g,h,i,l,m))");
    private static final SpliceTableWatcher timestampPk             = new SpliceTableWatcher("timestampPk"      ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(a,b,c,d,e,f,g,h,i,l,m,n))");

    /*
     * By reversing the order of the primary key, we ensure that the Key encoding and decoding maps get
     * re-ordered, which tests that we've set up the actual key and row decoding logic correctly
     */
    private static final SpliceTableWatcher smallintPkReversed      = new SpliceTableWatcher("smallintPkReversed"   ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(b,a))");
    private static final SpliceTableWatcher intPkReversed           = new SpliceTableWatcher("intPkReversed"        ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(c,b,a))");
    private static final SpliceTableWatcher bigintPkReversed        = new SpliceTableWatcher("bigintPkReversed"     ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(d,c,b,a))");
    private static final SpliceTableWatcher realPkReversed          = new SpliceTableWatcher("realPkReversed"       ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(e,d,c,b,a))");
    private static final SpliceTableWatcher doublePkReversed        = new SpliceTableWatcher("doublePkReversed"     ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(f,e,d,c,b,a))");
    private static final SpliceTableWatcher numericPkReversed       = new SpliceTableWatcher("numericPkReversed"    ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(g,f,e,d,c,b,a))");
    private static final SpliceTableWatcher charPkReversed          = new SpliceTableWatcher("charPkReversed"       ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(h,g,f,e,d,c,b,a))");
    private static final SpliceTableWatcher varcharPkReversed       = new SpliceTableWatcher("varcharPkReversed"    ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(i,h,g,f,e,d,c,b,a))");
    private static final SpliceTableWatcher datePkReversed          = new SpliceTableWatcher("datePkReversed"       ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(l,i,h,g,f,e,d,c,b,a))");
    private static final SpliceTableWatcher timePkReversed          = new SpliceTableWatcher("timePkReversed"       ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(m,l,i,h,g,f,e,d,c,b,a))");
    private static final SpliceTableWatcher timestampPkReversed     = new SpliceTableWatcher("timestampPkReversed"  ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(n,m,l,i,h,g,f,e,d,c,b,a))");

    @ClassRule public static final TestRule rule = RuleChain.outerRule(classWatcher)
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
            .around(smallintPkReversed)
            .around(intPkReversed)
            .around(bigintPkReversed)
            .around(realPkReversed)
            .around(doublePkReversed)
            .around(numericPkReversed)
            .around(charPkReversed)
            .around(varcharPkReversed)
            .around(datePkReversed)
            .around(timePkReversed)
            .around(timestampPkReversed)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try(CallableStatement cs=classWatcher.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
                        cs.setString(1,"SYS");
                        cs.execute();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description){
                    try(TestDataBuilder tdBuilder = new TestDataBuilder(schema.schemaName,INSERTION_SCHEMA,classWatcher.getOrCreateConnection(),size/8)){
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
                                .newTable(timestampPk.tableName)
                                .newTable(smallintPkReversed.tableName)
                                .newTable(intPkReversed.tableName)
                                .newTable(bigintPkReversed.tableName)
                                .newTable(realPkReversed.tableName)
                                .newTable(doublePkReversed.tableName)
                                .newTable(numericPkReversed.tableName)
                                .newTable(charPkReversed.tableName)
                                .newTable(varcharPkReversed.tableName)
                                .newTable(datePkReversed.tableName)
                                .newTable(timePkReversed.tableName)
                                .newTable(timestampPkReversed.tableName);
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
                        boolean boVal;

                        for(int i=0;i<size;i++){
                            daVal=new Date(i);
                            daCol.setMin(daVal);
                            tVal=new Time(i%2);
                            tCol.setMin(tVal);
                            tsVal=new Timestamp(i%4);
                            tsCol.setMin(tsVal);
                            boVal = i%2==0;
                            tdBuilder.booleanField(boVal)
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
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            });
    private static final String LANG_COLUMN_STATISTICS_NOT_POSSIBLE="SE020";
    private static final String LANG_DISABLE_STATS_FOR_KEYED_COLUMN="SE021";

    private static Connection conn;

    private static final TestColumn boCol = new TestColumn("a",false){
        @Override
        String getMinValueString(){
            return "true";
        }
    };
    private static final TestColumn siCol = new TestColumn("b",false);
    private static final TestColumn iCol = new TestColumn("c",false);
    private static final TestColumn biCol = new TestColumn("d",false);
    private static final TestColumn rCol = new TestColumn("e",true);
    private static final TestColumn dCol = new TestColumn("f",true);
    private static final TestColumn nCol = new TestColumn("g",false);
    private static final TestColumn cCol = new TestColumn("h",false);
    private static final TestColumn vcCol = new TestColumn("i",false);
    private static final TestColumn daCol = new TestColumn("l",false){
        Date min;
        @Override String getMinValueString(){ return min==null? "null":min.toString(); }

        @Override
        public void setMin(Object min){
            Date m = (Date)min;
            if(this.min==null||this.min.compareTo(m)>0)
                this.min = m;
        }
    };
    private static final TestColumn tCol = new TestColumn("m",false){
        Time min;
        @Override String getMinValueString(){ return min==null? "null":min.toString(); }
        @Override
        public void setMin(Object min){
            Time m = (Time)min;
            if(this.min==null||this.min.compareTo(m)>0)
                this.min = m;
        }
    };
    private static final TestColumn tsCol = new TestColumn("n",false){
        Timestamp min;
        @Override String getMinValueString(){ return min==null? "null":min.toString(); }
        @Override
        public void setMin(Object min){
            Timestamp m = (Timestamp)min;
            if(this.min==null||this.min.compareTo(m)>0)
                this.min = m;
        }
    };

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

    @Test public void noPk_boolean()           throws Exception{ testCorrect(allDataTypes.tableName,boCol); }
    @Test public void noPk_boolean_smallint()  throws Exception{ testCorrect(allDataTypes.tableName,boCol,siCol); }
    @Test public void noPk_boolean_int()       throws Exception{ testCorrect(allDataTypes.tableName,boCol,iCol); }
    @Test public void noPk_boolean_bigint()    throws Exception{ testCorrect(allDataTypes.tableName,boCol,biCol); }
    @Test public void noPk_boolean_real()      throws Exception{ testCorrect(allDataTypes.tableName,boCol,rCol); }
    @Test public void noPk_boolean_double()    throws Exception{ testCorrect(allDataTypes.tableName,boCol,dCol); }
    @Test public void noPk_boolean_numeric()   throws Exception{ testCorrect(allDataTypes.tableName,boCol,nCol); }
    @Test public void noPk_boolean_char()      throws Exception{ testCorrect(allDataTypes.tableName,boCol,cCol); }
    @Test public void noPk_boolean_varchar()   throws Exception{ testCorrect(allDataTypes.tableName,boCol,vcCol); }
    @Test public void noPk_boolean_date()      throws Exception{ testCorrect(allDataTypes.tableName,boCol,daCol); }
    @Test public void noPk_boolean_time()      throws Exception{ testCorrect(allDataTypes.tableName,boCol,tCol); }
    @Test public void noPk_boolean_timestamp() throws Exception{ testCorrect(allDataTypes.tableName,boCol,tsCol); }

    @Test public void noPk_smallint()           throws Exception{ testCorrect(allDataTypes.tableName,siCol); }
    @Test public void noPk_smallint_boolean()   throws Exception{ testCorrect(allDataTypes.tableName,siCol,boCol); }
    @Test public void noPk_smallint_int()       throws Exception{ testCorrect(allDataTypes.tableName,siCol,iCol); }
    @Test public void noPk_smallint_bigint()    throws Exception{ testCorrect(allDataTypes.tableName,siCol,biCol); }
    @Test public void noPk_smallint_real()      throws Exception{ testCorrect(allDataTypes.tableName,siCol,rCol); }
    @Test public void noPk_smallint_double()    throws Exception{ testCorrect(allDataTypes.tableName,siCol,dCol); }
    @Test public void noPk_smallint_numeric()   throws Exception{ testCorrect(allDataTypes.tableName,siCol,nCol); }
    @Test public void noPk_smallint_char()      throws Exception{ testCorrect(allDataTypes.tableName,siCol,cCol); }
    @Test public void noPk_smallint_varchar()   throws Exception{ testCorrect(allDataTypes.tableName,siCol,vcCol); }
    @Test public void noPk_smallint_date()      throws Exception{ testCorrect(allDataTypes.tableName,siCol,daCol); }
    @Test public void noPk_smallint_time()      throws Exception{ testCorrect(allDataTypes.tableName,siCol,tCol); }
    @Test public void noPk_smallint_timestamp() throws Exception{ testCorrect(allDataTypes.tableName,siCol,tsCol); }

    @Test public void noPk_int()                throws Exception{ testCorrect(allDataTypes.tableName,iCol); }
    @Test public void noPk_int_boolean()        throws Exception{ testCorrect(allDataTypes.tableName,iCol,boCol); }
    @Test public void noPk_int_smallint()       throws Exception{ testCorrect(allDataTypes.tableName,iCol,siCol); }
    @Test public void noPk_int_bigint()         throws Exception{ testCorrect(allDataTypes.tableName,iCol,biCol); }
    @Test public void noPk_int_real()           throws Exception{ testCorrect(allDataTypes.tableName,iCol,rCol); }
    @Test public void noPk_int_double()         throws Exception{ testCorrect(allDataTypes.tableName,iCol,dCol); }
    @Test public void noPk_int_numeric()        throws Exception{ testCorrect(allDataTypes.tableName,iCol,nCol); }
    @Test public void noPk_int_char()           throws Exception{ testCorrect(allDataTypes.tableName,iCol,cCol); }
    @Test public void noPk_int_varchar()        throws Exception{ testCorrect(allDataTypes.tableName,iCol,vcCol); }
    @Test public void noPk_int_date()           throws Exception{ testCorrect(allDataTypes.tableName,iCol,daCol); }
    @Test public void noPk_int_time()           throws Exception{ testCorrect(allDataTypes.tableName,iCol,tCol); }
    @Test public void noPk_int_timestamp()      throws Exception{ testCorrect(allDataTypes.tableName,iCol,tsCol); }

    @Test public void noPk_bigint()             throws Exception{ testCorrect(allDataTypes.tableName,biCol); }
    @Test public void noPk_bigint_boolean()     throws Exception{ testCorrect(allDataTypes.tableName,biCol,boCol); }
    @Test public void noPk_bigint_smallint()    throws Exception{ testCorrect(allDataTypes.tableName,biCol,siCol); }
    @Test public void noPk_bigint_int()         throws Exception{ testCorrect(allDataTypes.tableName,biCol,iCol); }
    @Test public void noPk_bigint_real()        throws Exception{ testCorrect(allDataTypes.tableName,biCol,rCol); }
    @Test public void noPk_bigint_double()      throws Exception{ testCorrect(allDataTypes.tableName,biCol,dCol); }
    @Test public void noPk_bigint_numeric()     throws Exception{ testCorrect(allDataTypes.tableName,biCol,nCol); }
    @Test public void noPk_bigint_char()        throws Exception{ testCorrect(allDataTypes.tableName,biCol,cCol); }
    @Test public void noPk_bigint_varchar()     throws Exception{ testCorrect(allDataTypes.tableName,biCol,vcCol); }
    @Test public void noPk_bigint_date()        throws Exception{ testCorrect(allDataTypes.tableName,biCol,daCol); }
    @Test public void noPk_bigint_time()        throws Exception{ testCorrect(allDataTypes.tableName,biCol,tCol); }
    @Test public void noPk_bigint_timestamp()   throws Exception{ testCorrect(allDataTypes.tableName,biCol,tsCol); }

    @Test public void noPk_real()               throws Exception{ testCorrect(allDataTypes.tableName,rCol); }
    @Test public void noPk_real_boolean()       throws Exception{ testCorrect(allDataTypes.tableName,rCol,boCol); }
    @Test public void noPk_real_smallint()      throws Exception{ testCorrect(allDataTypes.tableName,rCol,siCol); }
    @Test public void noPk_real_int()           throws Exception{ testCorrect(allDataTypes.tableName,rCol,iCol); }
    @Test public void noPk_real_bigint()        throws Exception{ testCorrect(allDataTypes.tableName,rCol,biCol); }
    @Test public void noPk_real_double()        throws Exception{ testCorrect(allDataTypes.tableName,rCol,dCol); }
    @Test public void noPk_real_numeric()       throws Exception{ testCorrect(allDataTypes.tableName,rCol,nCol); }
    @Test public void noPk_real_char()          throws Exception{ testCorrect(allDataTypes.tableName,rCol,cCol); }
    @Test public void noPk_real_varchar()       throws Exception{ testCorrect(allDataTypes.tableName,rCol,vcCol); }
    @Test public void noPk_real_date()          throws Exception{ testCorrect(allDataTypes.tableName,rCol,daCol); }
    @Test public void noPk_real_time()          throws Exception{ testCorrect(allDataTypes.tableName,rCol,tCol); }
    @Test public void noPk_real_timestamp()     throws Exception{ testCorrect(allDataTypes.tableName,rCol,tsCol); }

    @Test public void noPk_double()             throws Exception{ testCorrect(allDataTypes.tableName,dCol); }
    @Test public void noPk_double_boolean()     throws Exception{ testCorrect(allDataTypes.tableName,dCol,boCol); }
    @Test public void noPk_double_smallint()    throws Exception{ testCorrect(allDataTypes.tableName,dCol,siCol); }
    @Test public void noPk_double_int()         throws Exception{ testCorrect(allDataTypes.tableName,dCol,iCol); }
    @Test public void noPk_double_bigint()      throws Exception{ testCorrect(allDataTypes.tableName,dCol,biCol); }
    @Test public void noPk_double_real()        throws Exception{ testCorrect(allDataTypes.tableName,dCol,rCol); }
    @Test public void noPk_double_numeric()     throws Exception{ testCorrect(allDataTypes.tableName,dCol,nCol); }
    @Test public void noPk_double_char()        throws Exception{ testCorrect(allDataTypes.tableName,dCol,cCol); }
    @Test public void noPk_double_varchar()     throws Exception{ testCorrect(allDataTypes.tableName,dCol,vcCol); }
    @Test public void noPk_double_date()        throws Exception{ testCorrect(allDataTypes.tableName,dCol,daCol); }
    @Test public void noPk_double_time()        throws Exception{ testCorrect(allDataTypes.tableName,dCol,tCol); }
    @Test public void noPk_double_timestamp()   throws Exception{ testCorrect(allDataTypes.tableName,dCol,tsCol); }

    @Test public void noPk_numeric()            throws Exception{ testCorrect(allDataTypes.tableName,nCol); }
    @Test public void noPk_numeric_boolean()    throws Exception{ testCorrect(allDataTypes.tableName,nCol,boCol); }
    @Test public void noPk_numeric_smallint()   throws Exception{ testCorrect(allDataTypes.tableName,nCol,siCol); }
    @Test public void noPk_numeric_int()        throws Exception{ testCorrect(allDataTypes.tableName,nCol,iCol); }
    @Test public void noPk_numeric_bigint()     throws Exception{ testCorrect(allDataTypes.tableName,nCol,biCol); }
    @Test public void noPk_numeric_real()       throws Exception{ testCorrect(allDataTypes.tableName,nCol,rCol); }
    @Test public void noPk_numeric_double()     throws Exception{ testCorrect(allDataTypes.tableName,nCol,dCol); }
    @Test public void noPk_numeric_char()       throws Exception{ testCorrect(allDataTypes.tableName,nCol,cCol); }
    @Test public void noPk_numeric_varchar()    throws Exception{ testCorrect(allDataTypes.tableName,nCol,vcCol); }
    @Test public void noPk_numeric_date()       throws Exception{ testCorrect(allDataTypes.tableName,nCol,daCol); }
    @Test public void noPk_numeric_time()       throws Exception{ testCorrect(allDataTypes.tableName,nCol,tCol); }
    @Test public void noPk_numeric_timestamp()  throws Exception{ testCorrect(allDataTypes.tableName,nCol,tsCol); }

    @Test public void noPk_char()               throws Exception{ testCorrect(allDataTypes.tableName,cCol); }
    @Test public void noPk_char_boolean()       throws Exception{ testCorrect(allDataTypes.tableName,cCol,boCol); }
    @Test public void noPk_char_smallint()      throws Exception{ testCorrect(allDataTypes.tableName,cCol,siCol); }
    @Test public void noPk_char_int()           throws Exception{ testCorrect(allDataTypes.tableName,cCol,iCol); }
    @Test public void noPk_char_bigint()        throws Exception{ testCorrect(allDataTypes.tableName,cCol,biCol); }
    @Test public void noPk_char_real()          throws Exception{ testCorrect(allDataTypes.tableName,cCol,rCol); }
    @Test public void noPk_char_double()        throws Exception{ testCorrect(allDataTypes.tableName,cCol,dCol); }
    @Test public void noPk_char_numeric()       throws Exception{ testCorrect(allDataTypes.tableName,cCol,nCol); }
    @Test public void noPk_char_varchar()       throws Exception{ testCorrect(allDataTypes.tableName,cCol,vcCol); }
    @Test public void noPk_char_date()          throws Exception{ testCorrect(allDataTypes.tableName,cCol,daCol); }
    @Test public void noPk_char_time()          throws Exception{ testCorrect(allDataTypes.tableName,cCol,tCol); }
    @Test public void noPk_char_timestamp()     throws Exception{ testCorrect(allDataTypes.tableName,cCol,tsCol); }

    @Test public void noPk_varchar()            throws Exception{ testCorrect(allDataTypes.tableName,vcCol); }
    @Test public void noPk_varchar_boolean()    throws Exception{ testCorrect(allDataTypes.tableName,vcCol,boCol); }
    @Test public void noPk_varchar_smallint()   throws Exception{ testCorrect(allDataTypes.tableName,vcCol,siCol); }
    @Test public void noPk_varchar_int()        throws Exception{ testCorrect(allDataTypes.tableName,vcCol,iCol); }
    @Test public void noPk_varchar_bigint()     throws Exception{ testCorrect(allDataTypes.tableName,vcCol,biCol); }
    @Test public void noPk_varchar_real()       throws Exception{ testCorrect(allDataTypes.tableName,vcCol,rCol); }
    @Test public void noPk_varchar_double()     throws Exception{ testCorrect(allDataTypes.tableName,vcCol,dCol); }
    @Test public void noPk_varchar_numeric()    throws Exception{ testCorrect(allDataTypes.tableName,vcCol,nCol); }
    @Test public void noPk_varchar_char()       throws Exception{ testCorrect(allDataTypes.tableName,vcCol,cCol); }
    @Test public void noPk_varchar_date()       throws Exception{ testCorrect(allDataTypes.tableName,vcCol,daCol); }
    @Test public void noPk_varchar_time()       throws Exception{ testCorrect(allDataTypes.tableName,vcCol,tCol); }
    @Test public void noPk_varchar_timestamp()  throws Exception{ testCorrect(allDataTypes.tableName,vcCol,tsCol); }

    @Test public void noPk_date()               throws Exception{ testCorrect(allDataTypes.tableName,daCol); }
    @Test public void noPk_date_boolean()       throws Exception{ testCorrect(allDataTypes.tableName,daCol,boCol); }
    @Test public void noPk_date_smallint()      throws Exception{ testCorrect(allDataTypes.tableName,daCol,siCol); }
    @Test public void noPk_date_int()           throws Exception{ testCorrect(allDataTypes.tableName,daCol,iCol); }
    @Test public void noPk_date_bigint()        throws Exception{ testCorrect(allDataTypes.tableName,daCol,biCol); }
    @Test public void noPk_date_real()          throws Exception{ testCorrect(allDataTypes.tableName,daCol,rCol); }
    @Test public void noPk_date_double()        throws Exception{ testCorrect(allDataTypes.tableName,daCol,dCol); }
    @Test public void noPk_date_numeric()       throws Exception{ testCorrect(allDataTypes.tableName,daCol,nCol); }
    @Test public void noPk_date_char()          throws Exception{ testCorrect(allDataTypes.tableName,daCol,cCol); }
    @Test public void noPk_date_varchar()       throws Exception{ testCorrect(allDataTypes.tableName,daCol,vcCol); }
    @Test public void noPk_date_date()          throws Exception{ testCorrect(allDataTypes.tableName,daCol,tCol); }
    @Test public void noPk_date_timestamp()     throws Exception{ testCorrect(allDataTypes.tableName,daCol,tsCol); }

    @Test public void noPk_time()               throws Exception{ testCorrect(allDataTypes.tableName, tCol); }
    @Test public void noPk_time_boolean()       throws Exception{ testCorrect(allDataTypes.tableName,tCol,boCol); }
    @Test public void noPk_time_smallint()      throws Exception{ testCorrect(allDataTypes.tableName,tCol,siCol); }
    @Test public void noPk_time_int()           throws Exception{ testCorrect(allDataTypes.tableName,tCol,iCol); }
    @Test public void noPk_time_bigint()        throws Exception{ testCorrect(allDataTypes.tableName,tCol,biCol); }
    @Test public void noPk_time_real()          throws Exception{ testCorrect(allDataTypes.tableName,tCol,rCol); }
    @Test public void noPk_time_double()        throws Exception{ testCorrect(allDataTypes.tableName,tCol,dCol); }
    @Test public void noPk_time_numeric()       throws Exception{ testCorrect(allDataTypes.tableName,tCol,nCol); }
    @Test public void noPk_time_char()          throws Exception{ testCorrect(allDataTypes.tableName,tCol,cCol); }
    @Test public void noPk_time_varchar()       throws Exception{ testCorrect(allDataTypes.tableName,tCol,vcCol); }
    @Test public void noPk_time_date()          throws Exception{ testCorrect(allDataTypes.tableName,tCol,daCol); }
    @Test public void noPk_time_timestamp()     throws Exception{ testCorrect(allDataTypes.tableName,tCol,tsCol); }

    @Test public void noPk_timestamp()          throws Exception{ testCorrect(allDataTypes.tableName,tsCol); }
    @Test public void noPk_timestamp_boolean()  throws Exception{ testCorrect(allDataTypes.tableName,tsCol,boCol); }
    @Test public void noPk_timestamp_smallint() throws Exception{ testCorrect(allDataTypes.tableName,tsCol,siCol); }
    @Test public void noPk_timestamp_int()      throws Exception{ testCorrect(allDataTypes.tableName,tsCol,iCol); }
    @Test public void noPk_timestamp_bigint()   throws Exception{ testCorrect(allDataTypes.tableName,tsCol,biCol); }
    @Test public void noPk_timestamp_real()     throws Exception{ testCorrect(allDataTypes.tableName,tsCol,rCol); }
    @Test public void noPk_timestamp_double()   throws Exception{ testCorrect(allDataTypes.tableName,tsCol,dCol); }
    @Test public void noPk_timestamp_numeric()  throws Exception{ testCorrect(allDataTypes.tableName,tsCol,nCol); }
    @Test public void noPk_timestamp_char()     throws Exception{ testCorrect(allDataTypes.tableName,tsCol,cCol); }
    @Test public void noPk_timestamp_varchar()  throws Exception{ testCorrect(allDataTypes.tableName,tsCol,vcCol); }
    @Test public void noPk_timestamp_date()     throws Exception{ testCorrect(allDataTypes.tableName,tsCol,daCol); }
    @Test public void noPk_timestamp_time()     throws Exception{ testCorrect(allDataTypes.tableName,tsCol,tCol); }

    @Test public void pk_smallint()             throws Exception{ testCorrect(smallintPk.tableName,siCol); }
    @Test public void pk_smallint_boolean()     throws Exception{ testCorrect(smallintPk.tableName,siCol,boCol); }
    @Test public void pk_smallint_int()         throws Exception{ testCorrect(smallintPk.tableName,siCol,iCol); }
    @Test public void pk_smallint_bigint()      throws Exception{ testCorrect(smallintPk.tableName,siCol,biCol); }
    @Test public void pk_smallint_real()        throws Exception{ testCorrect(smallintPk.tableName,siCol,rCol); }
    @Test public void pk_smallint_double()      throws Exception{ testCorrect(smallintPk.tableName,siCol,dCol); }
    @Test public void pk_smallint_numeric()     throws Exception{ testCorrect(smallintPk.tableName,siCol,nCol); }
    @Test public void pk_smallint_char()        throws Exception{ testCorrect(smallintPk.tableName,siCol,cCol); }
    @Test public void pk_smallint_varchar()     throws Exception{ testCorrect(smallintPk.tableName,siCol,vcCol); }
    @Test public void pk_smallint_date()        throws Exception{ testCorrect(smallintPk.tableName,siCol,daCol); }
    @Test public void pk_smallint_time()        throws Exception{ testCorrect(smallintPk.tableName,siCol,tCol); }
    @Test public void pk_smallint_timestamp()   throws Exception{ testCorrect(smallintPk.tableName,siCol,tsCol); }

    @Test public void pk_smallint_r()           throws Exception{ testCorrect(smallintPkReversed.tableName,siCol); }
    @Test public void pk_smallint_r_boolean()   throws Exception{ testCorrect(smallintPkReversed.tableName,siCol,boCol); }
    @Test public void pk_smallint_r_int()       throws Exception{ testCorrect(smallintPkReversed.tableName,siCol,iCol); }
    @Test public void pk_smallint_r_bigint()    throws Exception{ testCorrect(smallintPkReversed.tableName,siCol,biCol); }
    @Test public void pk_smallint_r_real()      throws Exception{ testCorrect(smallintPkReversed.tableName,siCol,rCol); }
    @Test public void pk_smallint_r_double()    throws Exception{ testCorrect(smallintPkReversed.tableName,siCol,dCol); }
    @Test public void pk_smallint_r_numeric()   throws Exception{ testCorrect(smallintPkReversed.tableName,siCol,nCol); }
    @Test public void pk_smallint_r_char()      throws Exception{ testCorrect(smallintPkReversed.tableName,siCol,cCol); }
    @Test public void pk_smallint_r_varchar()   throws Exception{ testCorrect(smallintPkReversed.tableName,siCol,vcCol); }
    @Test public void pk_smallint_r_date()      throws Exception{ testCorrect(smallintPkReversed.tableName,siCol,daCol); }
    @Test public void pk_smallint_r_time()      throws Exception{ testCorrect(smallintPkReversed.tableName,siCol,tCol); }
    @Test public void pk_smallint_r_timestamp() throws Exception{ testCorrect(smallintPkReversed.tableName,siCol,tsCol); }

    @Test public void pk_int()                  throws Exception{ testCorrect(intPk.tableName,iCol); }
    @Test public void pk_int_boolean()          throws Exception{ testCorrect(intPk.tableName,iCol,boCol); }
    @Test public void pk_int_smallint()         throws Exception{ testCorrect(intPk.tableName,iCol,siCol); }
    @Test public void pk_int_bigint()           throws Exception{ testCorrect(intPk.tableName,iCol,biCol); }
    @Test public void pk_int_real()             throws Exception{ testCorrect(intPk.tableName,iCol,rCol); }
    @Test public void pk_int_double()           throws Exception{ testCorrect(intPk.tableName,iCol,dCol); }
    @Test public void pk_int_numeric()          throws Exception{ testCorrect(intPk.tableName,iCol,nCol); }
    @Test public void pk_int_char()             throws Exception{ testCorrect(intPk.tableName,iCol,cCol); }
    @Test public void pk_int_varchar()          throws Exception{ testCorrect(intPk.tableName,iCol,vcCol); }
    @Test public void pk_int_date()             throws Exception{ testCorrect(intPk.tableName,iCol,daCol); }
    @Test public void pk_int_time()             throws Exception{ testCorrect(intPk.tableName,iCol,tCol); }
    @Test public void pk_int_timestamp()        throws Exception{ testCorrect(intPk.tableName,iCol,tsCol); }

    @Test public void pk_int_r()                throws Exception{ testCorrect(intPkReversed.tableName, iCol); }
    @Test public void pk_int_r_boolean()        throws Exception{ testCorrect(intPkReversed.tableName,iCol,boCol); }
    @Test public void pk_int_r_smallint()       throws Exception{ testCorrect(intPkReversed.tableName,iCol,siCol); }
    @Test public void pk_int_r_bigint()         throws Exception{ testCorrect(intPkReversed.tableName,iCol,biCol); }
    @Test public void pk_int_r_real()           throws Exception{ testCorrect(intPkReversed.tableName,iCol,rCol); }
    @Test public void pk_int_r_double()         throws Exception{ testCorrect(intPkReversed.tableName,iCol,dCol); }
    @Test public void pk_int_r_numeric()        throws Exception{ testCorrect(intPkReversed.tableName,iCol,nCol); }
    @Test public void pk_int_r_char()           throws Exception{ testCorrect(intPkReversed.tableName,iCol,cCol); }
    @Test public void pk_int_r_varchar()        throws Exception{ testCorrect(intPkReversed.tableName,iCol,vcCol); }
    @Test public void pk_int_r_date()           throws Exception{ testCorrect(intPkReversed.tableName,iCol,daCol); }
    @Test public void pk_int_r_time()           throws Exception{ testCorrect(intPkReversed.tableName,iCol,tCol); }
    @Test public void pk_int_r_timestamp()      throws Exception{ testCorrect(intPkReversed.tableName,iCol,tsCol); }

    @Test public void pk_bigint()               throws Exception{ testCorrect(bigintPk.tableName,biCol); }
    @Test public void pk_bigint_boolean()       throws Exception{ testCorrect(bigintPk.tableName,biCol,boCol); }
    @Test public void pk_bigint_smallint()      throws Exception{ testCorrect(bigintPk.tableName,biCol,siCol); }
    @Test public void pk_bigint_int()           throws Exception{ testCorrect(bigintPk.tableName,biCol,iCol); }
    @Test public void pk_bigint_real()          throws Exception{ testCorrect(bigintPk.tableName,biCol,rCol); }
    @Test public void pk_bigint_double()        throws Exception{ testCorrect(bigintPk.tableName,biCol,dCol); }
    @Test public void pk_bigint_numeric()       throws Exception{ testCorrect(bigintPk.tableName,biCol,nCol); }
    @Test public void pk_bigint_char()          throws Exception{ testCorrect(bigintPk.tableName,biCol,cCol); }
    @Test public void pk_bigint_varchar()       throws Exception{ testCorrect(bigintPk.tableName,biCol,vcCol); }
    @Test public void pk_bigint_date()          throws Exception{ testCorrect(bigintPk.tableName,biCol,daCol); }
    @Test public void pk_bigint_time()          throws Exception{ testCorrect(bigintPk.tableName,biCol,tCol); }
    @Test public void pk_bigint_timestamp()     throws Exception{ testCorrect(bigintPk.tableName,biCol,tsCol); }

    @Test public void pk_bigint_r()             throws Exception{ testCorrect(bigintPkReversed.tableName,biCol); }
    @Test public void pk_bigint_r_boolean()     throws Exception{ testCorrect(bigintPkReversed.tableName,biCol,boCol); }
    @Test public void pk_bigint_r_smallint()    throws Exception{ testCorrect(bigintPkReversed.tableName,biCol,siCol); }
    @Test public void pk_bigint_r_int()         throws Exception{ testCorrect(bigintPkReversed.tableName,biCol,iCol); }
    @Test public void pk_bigint_r_real()        throws Exception{ testCorrect(bigintPkReversed.tableName,biCol,rCol); }
    @Test public void pk_bigint_r_double()      throws Exception{ testCorrect(bigintPkReversed.tableName,biCol,dCol); }
    @Test public void pk_bigint_r_numeric()     throws Exception{ testCorrect(bigintPkReversed.tableName,biCol,nCol); }
    @Test public void pk_bigint_r_char()        throws Exception{ testCorrect(bigintPkReversed.tableName,biCol,cCol); }
    @Test public void pk_bigint_r_varchar()     throws Exception{ testCorrect(bigintPkReversed.tableName,biCol,vcCol); }
    @Test public void pk_bigint_r_date()        throws Exception{ testCorrect(bigintPkReversed.tableName,biCol,daCol); }
    @Test public void pk_bigint_r_time()        throws Exception{ testCorrect(bigintPkReversed.tableName,biCol,tCol); }
    @Test public void pk_bigint_r_timestamp()   throws Exception{ testCorrect(bigintPkReversed.tableName,biCol,tsCol); }

    @Test public void pk_real()                 throws Exception{ testCorrect(realPk.tableName,rCol); }
    @Test public void pk_real_boolean()         throws Exception{ testCorrect(realPk.tableName,rCol,boCol); }
    @Test public void pk_real_smallint()        throws Exception{ testCorrect(realPk.tableName,rCol,siCol); }
    @Test public void pk_real_int()             throws Exception{ testCorrect(realPk.tableName,rCol,iCol); }
    @Test public void pk_real_bigint()          throws Exception{ testCorrect(realPk.tableName,rCol,biCol); }
    @Test public void pk_real_double()          throws Exception{ testCorrect(realPk.tableName,rCol,dCol); }
    @Test public void pk_real_numeric()         throws Exception{ testCorrect(realPk.tableName,rCol,nCol); }
    @Test public void pk_real_char()            throws Exception{ testCorrect(realPk.tableName,rCol,cCol); }
    @Test public void pk_real_varchar()         throws Exception{ testCorrect(realPk.tableName,rCol,vcCol); }
    @Test public void pk_real_date()            throws Exception{ testCorrect(realPk.tableName,rCol,daCol); }
    @Test public void pk_real_time()            throws Exception{ testCorrect(realPk.tableName,rCol,tCol); }
    @Test public void pk_real_timestamp()       throws Exception{ testCorrect(realPk.tableName,rCol,tsCol); }

    @Test public void pk_real_r()               throws Exception{ testCorrect(realPkReversed.tableName,rCol); }
    @Test public void pk_real_r_boolean()       throws Exception{ testCorrect(realPkReversed.tableName,rCol,boCol); }
    @Test public void pk_real_r_smallint()      throws Exception{ testCorrect(realPkReversed.tableName,rCol,siCol); }
    @Test public void pk_real_r_int()           throws Exception{ testCorrect(realPkReversed.tableName,rCol,iCol); }
    @Test public void pk_real_r_bigint()        throws Exception{ testCorrect(realPkReversed.tableName,rCol,biCol); }
    @Test public void pk_real_r_double()        throws Exception{ testCorrect(realPkReversed.tableName,rCol,dCol); }
    @Test public void pk_real_r_numeric()       throws Exception{ testCorrect(realPkReversed.tableName,rCol,nCol); }
    @Test public void pk_real_r_char()          throws Exception{ testCorrect(realPkReversed.tableName,rCol,cCol); }
    @Test public void pk_real_r_varchar()       throws Exception{ testCorrect(realPkReversed.tableName,rCol,vcCol); }
    @Test public void pk_real_r_date()          throws Exception{ testCorrect(realPkReversed.tableName,dCol,daCol); }
    @Test public void pk_real_r_time()          throws Exception{ testCorrect(realPkReversed.tableName,dCol,tCol); }
    @Test public void pk_real_r_timestamp()     throws Exception{ testCorrect(realPkReversed.tableName,dCol,tsCol); }

    @Test public void pk_double()               throws Exception{ testCorrect(doublePk.tableName,dCol); }
    @Test public void pk_double_boolean()       throws Exception{ testCorrect(doublePk.tableName,dCol,boCol); }
    @Test public void pk_double_smallint()      throws Exception{ testCorrect(doublePk.tableName,dCol,siCol); }
    @Test public void pk_double_int()           throws Exception{ testCorrect(doublePk.tableName,dCol,iCol); }
    @Test public void pk_double_bigint()        throws Exception{ testCorrect(doublePk.tableName,dCol,biCol); }
    @Test public void pk_double_real()          throws Exception{ testCorrect(doublePk.tableName,dCol,rCol); }
    @Test public void pk_double_numeric()       throws Exception{ testCorrect(doublePk.tableName,dCol,nCol); }
    @Test public void pk_double_char()          throws Exception{ testCorrect(doublePk.tableName,dCol,cCol); }
    @Test public void pk_double_varchar()       throws Exception{ testCorrect(doublePk.tableName,dCol,vcCol); }
    @Test public void pk_double_date()          throws Exception{ testCorrect(doublePk.tableName,dCol,daCol); }
    @Test public void pk_double_time()          throws Exception{ testCorrect(doublePk.tableName,dCol,tCol); }
    @Test public void pk_double_timestamp()     throws Exception{ testCorrect(doublePk.tableName,dCol,tsCol); }

    @Test public void pk_double_r()             throws Exception{ testCorrect(doublePkReversed.tableName, dCol); }
    @Test public void pk_double_r_boolean()     throws Exception{ testCorrect(doublePkReversed.tableName,dCol,boCol); }
    @Test public void pk_double_r_smallint()    throws Exception{ testCorrect(doublePkReversed.tableName,dCol,siCol); }
    @Test public void pk_double_r_int()         throws Exception{ testCorrect(doublePkReversed.tableName,dCol,iCol); }
    @Test public void pk_double_r_bigint()      throws Exception{ testCorrect(doublePkReversed.tableName,dCol,biCol); }
    @Test public void pk_double_r_real()        throws Exception{ testCorrect(doublePkReversed.tableName,dCol,rCol); }
    @Test public void pk_double_r_numeric()     throws Exception{ testCorrect(doublePkReversed.tableName,dCol,nCol); }
    @Test public void pk_double_r_char()        throws Exception{ testCorrect(doublePkReversed.tableName,dCol,cCol); }
    @Test public void pk_double_r_varchar()     throws Exception{ testCorrect(doublePkReversed.tableName,dCol,vcCol); }
    @Test public void pk_double_r_date()        throws Exception{ testCorrect(doublePkReversed.tableName,dCol,daCol); }
    @Test public void pk_double_r_time()        throws Exception{ testCorrect(doublePkReversed.tableName,dCol,tCol); }
    @Test public void pk_double_r_timestamp()   throws Exception{ testCorrect(doublePkReversed.tableName,dCol,tsCol); }

    @Test public void pk_numeric()              throws Exception{ testCorrect(numericPk.tableName,nCol); }
    @Test public void pk_numeric_boolean()      throws Exception{ testCorrect(numericPk.tableName,nCol,boCol); }
    @Test public void pk_numeric_smallint()     throws Exception{ testCorrect(numericPk.tableName,nCol,siCol); }
    @Test public void pk_numeric_int()          throws Exception{ testCorrect(numericPk.tableName,nCol,iCol); }
    @Test public void pk_numeric_bigint()       throws Exception{ testCorrect(numericPk.tableName,nCol,biCol); }
    @Test public void pk_numeric_real()         throws Exception{ testCorrect(numericPk.tableName,nCol,rCol); }
    @Test public void pk_numeric_double()       throws Exception{ testCorrect(numericPk.tableName,nCol,dCol); }
    @Test public void pk_numeric_char()         throws Exception{ testCorrect(numericPk.tableName,nCol,cCol); }
    @Test public void pk_numeric_varchar()      throws Exception{ testCorrect(numericPk.tableName,nCol,vcCol); }
    @Test public void pk_numeric_date()         throws Exception{ testCorrect(numericPk.tableName,nCol,daCol); }
    @Test public void pk_numeric_time()         throws Exception{ testCorrect(numericPk.tableName,nCol,tCol); }
    @Test public void pk_numeric_timestamp()    throws Exception{ testCorrect(numericPk.tableName,nCol,tsCol); }

    @Test public void pk_numeric_r()            throws Exception{ testCorrect(numericPkReversed.tableName,nCol); }
    @Test public void pk_numeric_r_boolean()    throws Exception{ testCorrect(numericPkReversed.tableName,nCol,boCol); }
    @Test public void pk_numeric_r_smallint()   throws Exception{ testCorrect(numericPkReversed.tableName,nCol,siCol); }
    @Test public void pk_numeric_r_int()        throws Exception{ testCorrect(numericPkReversed.tableName,nCol,iCol); }
    @Test public void pk_numeric_r_bigint()     throws Exception{ testCorrect(numericPkReversed.tableName,nCol,biCol); }
    @Test public void pk_numeric_r_real()       throws Exception{ testCorrect(numericPkReversed.tableName,nCol,rCol); }
    @Test public void pk_numeric_r_double()     throws Exception{ testCorrect(numericPkReversed.tableName,nCol,dCol); }
    @Test public void pk_numeric_r_char()       throws Exception{ testCorrect(numericPkReversed.tableName,nCol,cCol); }
    @Test public void pk_numeric_r_varchar()    throws Exception{ testCorrect(numericPkReversed.tableName,nCol,vcCol); }
    @Test public void pk_numeric_r_date()       throws Exception{ testCorrect(numericPkReversed.tableName,nCol,daCol); }
    @Test public void pk_numeric_r_time()       throws Exception{ testCorrect(numericPkReversed.tableName,nCol,tCol); }
    @Test public void pk_numeric_r_timestamp()  throws Exception{ testCorrect(numericPkReversed.tableName,nCol,tsCol); }

    @Test public void pk_char()                 throws Exception{ testCorrect(charPk.tableName,cCol); }
    @Test public void pk_char_boolean()         throws Exception{ testCorrect(charPk.tableName,cCol,boCol); }
    @Test public void pk_char_smallint()        throws Exception{ testCorrect(charPk.tableName,cCol,siCol); }
    @Test public void pk_char_int()             throws Exception{ testCorrect(charPk.tableName,cCol,iCol); }
    @Test public void pk_char_bigint()          throws Exception{ testCorrect(charPk.tableName,cCol,biCol); }
    @Test public void pk_char_real()            throws Exception{ testCorrect(charPk.tableName,cCol,rCol); }
    @Test public void pk_char_double()          throws Exception{ testCorrect(charPk.tableName,cCol,dCol); }
    @Test public void pk_char_numeric()         throws Exception{ testCorrect(charPk.tableName,cCol,nCol); }
    @Test public void pk_char_varchar()         throws Exception{ testCorrect(charPk.tableName,cCol,vcCol); }
    @Test public void pk_char_date()            throws Exception{ testCorrect(charPk.tableName,cCol,daCol); }
    @Test public void pk_char_time()            throws Exception{ testCorrect(charPk.tableName,cCol,tCol); }
    @Test public void pk_char_timestamp()       throws Exception{ testCorrect(charPk.tableName,cCol,tsCol); }

    @Test public void pk_char_r()               throws Exception{ testCorrect(charPkReversed.tableName,cCol); }
    @Test public void pk_char_r_boolean()       throws Exception{ testCorrect(charPkReversed.tableName,cCol,boCol); }
    @Test public void pk_char_r_smallint()      throws Exception{ testCorrect(charPkReversed.tableName,cCol,siCol); }
    @Test public void pk_char_r_int()           throws Exception{ testCorrect(charPkReversed.tableName,cCol,iCol); }
    @Test public void pk_char_r_bigint()        throws Exception{ testCorrect(charPkReversed.tableName,cCol,biCol); }
    @Test public void pk_char_r_real()          throws Exception{ testCorrect(charPkReversed.tableName,cCol,rCol); }
    @Test public void pk_char_r_double()        throws Exception{ testCorrect(charPkReversed.tableName,cCol,dCol); }
    @Test public void pk_char_r_numeric()       throws Exception{ testCorrect(charPkReversed.tableName,cCol,nCol); }
    @Test public void pk_char_r_varchar()       throws Exception{ testCorrect(charPkReversed.tableName,cCol,vcCol); }
    @Test public void pk_char_r_date()          throws Exception{ testCorrect(charPkReversed.tableName,cCol,daCol); }
    @Test public void pk_char_r_time()          throws Exception{ testCorrect(charPkReversed.tableName,cCol,tCol); }
    @Test public void pk_char_r_timestamp()     throws Exception{ testCorrect(charPkReversed.tableName,cCol,tsCol); }

    @Test public void pk_varchar()              throws Exception{ testCorrect(varcharPk.tableName,vcCol); }
    @Test public void pk_varchar_boolean()      throws Exception{ testCorrect(varcharPk.tableName,vcCol,boCol); }
    @Test public void pk_varchar_smallint()     throws Exception{ testCorrect(varcharPk.tableName,vcCol,siCol); }
    @Test public void pk_varchar_int()          throws Exception{ testCorrect(varcharPk.tableName,vcCol,iCol); }
    @Test public void pk_varchar_bigint()       throws Exception{ testCorrect(varcharPk.tableName,vcCol,biCol); }
    @Test public void pk_varchar_real()         throws Exception{ testCorrect(varcharPk.tableName,vcCol,rCol); }
    @Test public void pk_varchar_double()       throws Exception{ testCorrect(varcharPk.tableName,vcCol,dCol); }
    @Test public void pk_varchar_numeric()      throws Exception{ testCorrect(varcharPk.tableName,vcCol,nCol); }
    @Test public void pk_varchar_char()         throws Exception{ testCorrect(varcharPk.tableName,vcCol,cCol); }
    @Test public void pk_varchar_date()         throws Exception{ testCorrect(varcharPk.tableName,vcCol,daCol); }
    @Test public void pk_varchar_time()         throws Exception{ testCorrect(varcharPk.tableName,vcCol,tCol); }
    @Test public void pk_varchar_timestamp()    throws Exception{ testCorrect(varcharPk.tableName,vcCol,tsCol); }

    @Test public void pk_varchar_r()            throws Exception{ testCorrect(varcharPkReversed.tableName,vcCol); }
    @Test public void pk_varchar_r_boolean()    throws Exception{ testCorrect(varcharPkReversed.tableName,vcCol,boCol); }
    @Test public void pk_varchar_r_smallint()   throws Exception{ testCorrect(varcharPkReversed.tableName,vcCol,siCol); }
    @Test public void pk_varchar_r_int()        throws Exception{ testCorrect(varcharPkReversed.tableName,vcCol,iCol); }
    @Test public void pk_varchar_r_bigint()     throws Exception{ testCorrect(varcharPkReversed.tableName,vcCol,biCol); }
    @Test public void pk_varchar_r_real()       throws Exception{ testCorrect(varcharPkReversed.tableName,vcCol,rCol); }
    @Test public void pk_varchar_r_double()     throws Exception{ testCorrect(varcharPkReversed.tableName,vcCol,dCol); }
    @Test public void pk_varchar_r_numeric()    throws Exception{ testCorrect(varcharPkReversed.tableName,vcCol,nCol); }
    @Test public void pk_varchar_r_char()       throws Exception{ testCorrect(varcharPkReversed.tableName,vcCol,cCol); }
    @Test public void pk_varchar_r_date()       throws Exception{ testCorrect(varcharPkReversed.tableName,vcCol,daCol); }
    @Test public void pk_varchar_r_time()       throws Exception{ testCorrect(varcharPkReversed.tableName,vcCol,tCol); }
    @Test public void pk_varchar_r_timestamp()  throws Exception{ testCorrect(varcharPkReversed.tableName,vcCol,tsCol); }

    @Test public void pk_date()                 throws Exception{ testCorrect(datePk.tableName,daCol); }
    @Test public void pk_date_boolean()         throws Exception{ testCorrect(datePk.tableName,daCol,boCol); }
    @Test public void pk_date_smallint()        throws Exception{ testCorrect(datePk.tableName,daCol,siCol); }
    @Test public void pk_date_int()             throws Exception{ testCorrect(datePk.tableName,daCol,iCol); }
    @Test public void pk_date_bigint()          throws Exception{ testCorrect(datePk.tableName,daCol,biCol); }
    @Test public void pk_date_real()            throws Exception{ testCorrect(datePk.tableName,daCol,rCol); }
    @Test public void pk_date_double()          throws Exception{ testCorrect(datePk.tableName,daCol,dCol); }
    @Test public void pk_date_numeric()         throws Exception{ testCorrect(datePk.tableName,daCol,nCol); }
    @Test public void pk_date_char()            throws Exception{ testCorrect(datePk.tableName,daCol,cCol); }
    @Test public void pk_date_varchar()         throws Exception{ testCorrect(datePk.tableName,daCol,vcCol); }
    @Test public void pk_date_time()            throws Exception{ testCorrect(datePk.tableName,daCol,tCol); }
    @Test public void pk_date_timestamp()       throws Exception{ testCorrect(datePk.tableName,daCol,tsCol); }

    @Test public void pk_time()                 throws Exception{ testCorrect(timePk.tableName,tCol); }
    @Test public void pk_time_boolean()         throws Exception{ testCorrect(timePk.tableName,tCol,boCol); }
    @Test public void pk_time_smallint()        throws Exception{ testCorrect(timePk.tableName,tCol,siCol); }
    @Test public void pk_time_int()             throws Exception{ testCorrect(timePk.tableName,tCol,iCol); }
    @Test public void pk_time_bigint()          throws Exception{ testCorrect(timePk.tableName,tCol,biCol); }
    @Test public void pk_time_real()            throws Exception{ testCorrect(timePk.tableName,tCol,rCol); }
    @Test public void pk_time_double()          throws Exception{ testCorrect(timePk.tableName,tCol,dCol); }
    @Test public void pk_time_numeric()         throws Exception{ testCorrect(timePk.tableName,tCol,nCol); }
    @Test public void pk_time_char()            throws Exception{ testCorrect(timePk.tableName,tCol,cCol); }
    @Test public void pk_time_varchar()         throws Exception{ testCorrect(timePk.tableName,tCol,vcCol); }
    @Test public void pk_time_date()            throws Exception{ testCorrect(timePk.tableName,tCol,daCol); }
    @Test public void pk_time_timestamp()       throws Exception{ testCorrect(timePk.tableName,tCol,tsCol); }

    @Test public void pk_timestamp()            throws Exception{ testCorrect(timestampPk.tableName,tsCol); }
    @Test public void pk_timestamp_boolean()    throws Exception{ testCorrect(timestampPk.tableName,tsCol,boCol); }
    @Test public void pk_timestamp_smallint()   throws Exception{ testCorrect(timestampPk.tableName,tsCol,siCol); }
    @Test public void pk_timestamp_int()        throws Exception{ testCorrect(timestampPk.tableName,tsCol,iCol); }
    @Test public void pk_timestamp_bigint()     throws Exception{ testCorrect(timestampPk.tableName,tsCol,biCol); }
    @Test public void pk_timestamp_real()       throws Exception{ testCorrect(timestampPk.tableName,tsCol,rCol); }
    @Test public void pk_timestamp_double()     throws Exception{ testCorrect(timestampPk.tableName,tsCol,dCol); }
    @Test public void pk_timestamp_numeric()    throws Exception{ testCorrect(timestampPk.tableName,tsCol,nCol); }
    @Test public void pk_timestamp_char()       throws Exception{ testCorrect(timestampPk.tableName,tsCol,cCol); }
    @Test public void pk_timestamp_varchar()    throws Exception{ testCorrect(timestampPk.tableName,tsCol,vcCol); }
    @Test public void pk_timestamp_date()       throws Exception{ testCorrect(timestampPk.tableName,tsCol,daCol); }
    @Test public void pk_timestamp_time()       throws Exception{ testCorrect(timestampPk.tableName,tsCol,tCol); }

    @Test public void pk_date_r()               throws Exception{ testCorrect(datePkReversed.tableName,daCol); }
    @Test public void pk_date_r_boolean()       throws Exception{ testCorrect(datePkReversed.tableName,daCol,boCol); }
    @Test public void pk_date_r_smallint()      throws Exception{ testCorrect(datePkReversed.tableName,daCol,siCol); }
    @Test public void pk_date_r_int()           throws Exception{ testCorrect(datePkReversed.tableName,daCol,iCol); }
    @Test public void pk_date_r_bigint()        throws Exception{ testCorrect(datePkReversed.tableName,daCol,biCol); }
    @Test public void pk_date_r_real()          throws Exception{ testCorrect(datePkReversed.tableName,daCol,rCol); }
    @Test public void pk_date_r_double()        throws Exception{ testCorrect(datePkReversed.tableName,daCol,dCol); }
    @Test public void pk_date_r_numeric()       throws Exception{ testCorrect(datePkReversed.tableName,daCol,nCol); }
    @Test public void pk_date_r_char()          throws Exception{ testCorrect(datePkReversed.tableName,daCol,cCol); }
    @Test public void pk_date_r_varchar()       throws Exception{ testCorrect(datePkReversed.tableName,daCol,vcCol); }
    @Test public void pk_date_r_time()          throws Exception{ testCorrect(datePkReversed.tableName,daCol,tCol); }
    @Test public void pk_date_r_timestamp()     throws Exception{ testCorrect(datePkReversed.tableName,daCol,tsCol); }

    @Test public void pk_time_r()               throws Exception{ testCorrect(timePkReversed.tableName,tCol); }
    @Test public void pk_time_r_boolean()       throws Exception{ testCorrect(timePkReversed.tableName,tCol,boCol); }
    @Test public void pk_time_r_smallint()      throws Exception{ testCorrect(timePkReversed.tableName,tCol,siCol); }
    @Test public void pk_time_r_int()           throws Exception{ testCorrect(timePkReversed.tableName,tCol,iCol); }
    @Test public void pk_time_r_bigint()        throws Exception{ testCorrect(timePkReversed.tableName,tCol,biCol); }
    @Test public void pk_time_r_real()          throws Exception{ testCorrect(timePkReversed.tableName,tCol,rCol); }
    @Test public void pk_time_r_double()        throws Exception{ testCorrect(timePkReversed.tableName,tCol,dCol); }
    @Test public void pk_time_r_numeric()       throws Exception{ testCorrect(timePkReversed.tableName,tCol,nCol); }
    @Test public void pk_time_r_char()          throws Exception{ testCorrect(timePkReversed.tableName,tCol,cCol); }
    @Test public void pk_time_r_varchar()       throws Exception{ testCorrect(timePkReversed.tableName,tCol,vcCol); }
    @Test public void pk_time_r_date()          throws Exception{ testCorrect(timePkReversed.tableName,tCol,daCol); }
    @Test public void pk_time_r_timestamp()     throws Exception{ testCorrect(timePkReversed.tableName,tCol,tsCol); }

    @Test public void pk_timestamp_r()          throws Exception{ testCorrect(timestampPkReversed.tableName,tsCol); }
    @Test public void pk_timestamp_r_boolean()  throws Exception{ testCorrect(timestampPkReversed.tableName,tsCol,boCol); }
    @Test public void pk_timestamp_r_smallint() throws Exception{ testCorrect(timestampPkReversed.tableName,tsCol,siCol); }
    @Test public void pk_timestamp_r_int()      throws Exception{ testCorrect(timestampPkReversed.tableName,tsCol,iCol); }
    @Test public void pk_timestamp_r_bigint()   throws Exception{ testCorrect(timestampPkReversed.tableName,tsCol,biCol); }
    @Test public void pk_timestamp_r_real()     throws Exception{ testCorrect(timestampPkReversed.tableName,tsCol,rCol); }
    @Test public void pk_timestamp_r_double()   throws Exception{ testCorrect(timestampPkReversed.tableName,tsCol,dCol); }
    @Test public void pk_timestamp_r_numeric()  throws Exception{ testCorrect(timestampPkReversed.tableName,tsCol,nCol); }
    @Test public void pk_timestamp_r_char()     throws Exception{ testCorrect(timestampPkReversed.tableName,tsCol,cCol); }
    @Test public void pk_timestamp_r_varchar()  throws Exception{ testCorrect(timestampPkReversed.tableName,tsCol,vcCol); }
    @Test public void pk_timestamp_r_date()     throws Exception{ testCorrect(timestampPkReversed.tableName,tsCol,daCol); }
    @Test public void pk_timestamp_r_time()     throws Exception{ testCorrect(timestampPkReversed.tableName,tsCol,tCol); }

    /* ****************************************************************************************************************/
    /*Error handling tests*/

    @Test(expected = SQLException.class)
    public void cannotEnableStatsOnClob() throws Exception{
        try{
            enable(allDataTypes.tableName,"k");
            Assert.fail("No Exception thrown!");
        }catch(SQLException se){
            assertCodeCorrect(LANG_COLUMN_STATISTICS_NOT_POSSIBLE,se);
            throw se;
        }
    }

    @Test(expected = SQLException.class)
    public void cannotEnableStatsOnBlob() throws Exception{
        try{
            enable(allDataTypes.tableName,"j");
            Assert.fail("No Exception thrown!");
        }catch(SQLException se){
            assertCodeCorrect(LANG_COLUMN_STATISTICS_NOT_POSSIBLE,se);
            throw se;
        }
    }

    @Test(expected = SQLException.class)
    public void enableColumnDoesNotExist() throws Exception{
        try{
            enable(allDataTypes.tableName,"doesnotexist");
            Assert.fail("No Exception thrown!");
        }catch(SQLException se){
            assertCodeCorrect(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE,se);
            throw se;
        }
    }

    @Test(expected=SQLException.class)
    public void disableKeyedColumn() throws Exception{
        try(CallableStatement cs =conn.prepareCall("call SYSCS_UTIL.DISABLE_COLUMN_STATISTICS(?,?,?)")){
            cs.setString(1,schema.schemaName);
            cs.setString(2,smallintPk.tableName);
            cs.setString(3,"b");

            cs.execute();
            Assert.fail("No Exception thrown!");
        }catch(SQLException se){
            assertCodeCorrect(LANG_DISABLE_STATS_FOR_KEYED_COLUMN,se);
            throw se;
        }
    }

    @Test(expected=SQLException.class)
    public void disableColumnNotExists() throws Exception{
        try(CallableStatement cs =conn.prepareCall("call SYSCS_UTIL.DISABLE_COLUMN_STATISTICS(?,?,?)")){
            cs.setString(1,schema.schemaName);
            cs.setString(2,smallintPk.tableName);
            cs.setString(3,"doesnotexist");

            cs.execute();
            Assert.fail("No Exception thrown!");
        }catch(SQLException se){
            assertCodeCorrect(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE,se);
            throw se;
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private void assertCodeCorrect(String error,SQLException se){
        System.out.println(se.getMessage());
        Assert.assertEquals("Incorrect SQL state!",error,se.getSQLState());
    }
    private void enable(String tableName,String columnName) throws SQLException {
        try(CallableStatement enableCall = conn.prepareCall("call SYSCS_UTIL.ENABLE_COLUMN_STATISTICS(?,?,?)")){
            enableCall.setString(1,schema.schemaName);
            enableCall.setString(2,tableName);
            enableCall.setString(3,columnName.toUpperCase());
            enableCall.execute();
        }
    }

    private void assertCorrectCollectResults(String tableName,ResultSet results) throws SQLException {
        Assert.assertTrue("No rows returned!",results.next());
        Assert.assertEquals("Incorrect schema name!", schema.schemaName, results.getString(1));
        Assert.assertEquals("Incorrect table name!", tableName, results.getString(2));
//        Assert.assertEquals("Incorrect # of Regions collected!", 1, results.getInt(3));
//        Assert.assertEquals("Incorrect # of tasks executed!", 1, results.getInt(4));
        Assert.assertEquals("Incorrect # of rows collected!",size,results.getInt(4));
        Assert.assertFalse("More than one row returned!",results.next());
    }

    private void assertCorrectTableStatistics(String tableName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select * " +
                "from sysvw.systablestatistics where schemaname = ? and tablename = ?")) {
            ps.setString(1, schema.schemaName);
            ps.setString(2, tableName);

            try(ResultSet resultSet = ps.executeQuery()) {
                Assert.assertTrue("No rows returned!", resultSet.next());
                Assert.assertEquals("Incorrect schema returned!", schema.schemaName, resultSet.getString(1));
                Assert.assertEquals("Incorrect table returned!", tableName, resultSet.getString(2));
                Assert.assertEquals("Incorrect total row count!", size, resultSet.getInt(4));
                Assert.assertEquals("Incorrect average row count!", size, resultSet.getInt(5));
                Assert.assertEquals("Incorrect number of partitions!", 1, resultSet.getInt(7));
                Assert.assertFalse("More than one row returned!", resultSet.next());
            }
        }
    }

    private void assertColumnStatsCorrect(String tableName,TestColumn col) throws SQLException {
        String colName = col.columnName.toUpperCase();
        String minValue = col.getMinValueString();
        try(PreparedStatement ps = conn.prepareStatement("select * from " +
                "sysvw.syscolumnstatistics where schemaname = ? and tablename = ? and columnName=?")){
            ps.setString(1,schema.schemaName);
            ps.setString(2,tableName);
            ps.setString(3,colName);

            try(ResultSet rs = ps.executeQuery()){
                Assert.assertTrue("No rows returned!",rs.next());
                Assert.assertEquals("Incorrect schema!",schema.schemaName,rs.getString(1));
                Assert.assertEquals("Incorrect table!", tableName, rs.getString(2));
                Assert.assertEquals("Incorrect Column!", colName, rs.getString(3));
                Assert.assertEquals("Incorrect Null Count!",0,rs.getLong(5));
                Assert.assertEquals("Incorrect Min!",minValue,rs.getString(7).trim());
                Assert.assertFalse("More than one row returned!",rs.next());
            }
        }
    }

    private void testCorrect(String tableName, TestColumn... columns) throws SQLException {
        for(TestColumn column:columns) {
            enable(tableName, column.columnName);
        }
        try(CallableStatement collectCall = conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,false)")) {
            collectCall.setString(1, schema.schemaName);
            collectCall.setString(2, tableName);
            collectCall.execute();
            try(ResultSet results = collectCall.getResultSet()) {
                assertCorrectCollectResults(tableName, results);
            }
        }

        //now check that tableStats are correct
        assertCorrectTableStatistics(tableName);

        //now check that the column stats are correct
        for(TestColumn column:columns) {
            assertColumnStatsCorrect(tableName, column);
        }
    }

    private static class TestColumn{
        String columnName;
        boolean useFloatStrings;
        public String minValueString;

        public static TestColumn create(String columnName,boolean useFloatStrings){
            return new TestColumn(columnName,useFloatStrings);
        }
        public TestColumn(String columnName, boolean useFloatStrings) {
            this.columnName = columnName;
            this.useFloatStrings = useFloatStrings;
            this.minValueString = useFloatStrings?"0.0":"0";
        }

        public void setMin(Object min){
            //no-op
        }

        String getMinValueString(){
            return minValueString;
        }
    }

}
