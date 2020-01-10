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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.primitives.Bytes;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

/**
 * A Collection of ITs oriented around scanning and inserting into our own table.
 *
 * This is in HBase-SQL to allow for splitting and
 * @author Scott Fines
 *         Date: 3/3/16
 */
public class SelfInsertIT{
    private static SpliceWatcher classWatcher = new SpliceWatcher();

    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(SelfInsertIT.class.getSimpleName().toUpperCase());

    private static final SpliceTableWatcher splitTable = new SpliceTableWatcher("splice",schema.schemaName,"(a int, b bigint, c varchar(2000)) --SPLICE-PROPERTIES partitionSize=16\n");
    private static final SpliceTableWatcher pt = new SpliceTableWatcher("pt",schema.schemaName,"(a int)");

    private static String REALLY_LONG_GARBAGE_STRING;

    @ClassRule
    public static TestRule chain  =RuleChain.outerRule(classWatcher)
            .around(schema)
            .around(splitTable)
            .around(pt);


    private TestConnection conn;

    @BeforeClass
    public static void setUpClass() throws Exception{
        Random r = new Random(0);
        byte[] bytes = new byte[1000];
        r.nextBytes(bytes);
        REALLY_LONG_GARBAGE_STRING =Bytes.toHex(bytes); //generate an arbitrary garbage string
    }

    @Before
    public void setUp() throws Exception{
        conn = classWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDown() throws Exception{
        conn.rollback();
        conn.reset();
    }

    @Test
    public void testInsertCountMatchesRowCountNoSpark() throws Exception{
        int maxLevel = 16;
        try(PreparedStatement ps = conn.prepareStatement("select count(*) from "+splitTable)){
            try(Statement s=conn.createStatement()){
                String sql = "insert into "+splitTable+"(a,b,c) values (1,1,'"+REALLY_LONG_GARBAGE_STRING+"')";
                int updateCount = s.executeUpdate(sql);
                Assert.assertEquals("Incorrect update count!",1,updateCount);
                try(ResultSet rs = ps.executeQuery()){
                    Assert.assertTrue("No rows returned from count query!",rs.next());
                    Assert.assertEquals("Incorrect table size!",1l,rs.getLong(1));
                }

                for(int i=0;i<maxLevel;i++){
                    long newSize = 1l<<i;
                    System.out.println("inserting "+newSize+" records");
                    sql = "insert into "+splitTable+"(a,b,c) select a,b+"+newSize+",c from "+ splitTable+" --SPLICE-PROPERTIES useSpark=false\n";
                    updateCount = s.executeUpdate(sql);
                    Assert.assertEquals("Incorrect reported update count!",newSize,updateCount);
                    try(ResultSet rs = ps.executeQuery()){
                        Assert.assertTrue("No rows returned from count query!",rs.next());
                        Assert.assertEquals("Incorrect table count!",newSize<<1,rs.getLong(1));
                    }
                }
            }
        }
    }


    @Test
    public void testInsertParallelTasks() throws Exception{
        try(PreparedStatement ps = conn.prepareStatement("select count(*) from "+pt)){
            try(Statement s=conn.createStatement()){
                String sql = "insert into "+pt+"(a) values (1)";
                int updateCount = s.executeUpdate(sql);
                Assert.assertEquals("Incorrect update count!",1,updateCount);


                try(ResultSet rs = ps.executeQuery()){
                    Assert.assertTrue("No rows returned from count query!",rs.next());
                    Assert.assertEquals("Incorrect table size!",1l,rs.getLong(1));
                }

                String select = "select a from "+ pt+" --SPLICE-PROPERTIES useSpark=true\n";
                String union = " union all ";

                sql =  "insert into "+ pt +"(a) " + select + union + select + union + select + union + select + union + select;
                updateCount = s.executeUpdate(sql);

                Assert.assertEquals("Incorrect update count!",5,updateCount);
            }
        }
    }

}
