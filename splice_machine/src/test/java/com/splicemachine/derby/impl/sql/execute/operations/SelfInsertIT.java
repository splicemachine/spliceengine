/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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

    private static String REALLY_LONG_GARBAGE_STRING;

    @ClassRule
    public static TestRule chain  =RuleChain.outerRule(classWatcher)
            .around(schema)
            .around(splitTable);


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

}
