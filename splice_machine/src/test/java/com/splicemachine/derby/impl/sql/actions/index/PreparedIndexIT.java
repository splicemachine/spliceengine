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

package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.RuledConnection;
import com.splicemachine.derby.test.framework.SchemaRule;
import com.splicemachine.derby.test.framework.TableRule;
import com.splicemachine.derby.test.framework.TestConnectionPool;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * Tests for interacting with Indices and prepared statements.
 *
 * @author Scott Fines
 *         Date: 6/29/16
 */
public class PreparedIndexIT{
    private static final TestConnectionPool connPool = new TestConnectionPool();
    private static final String SCHEMA = PreparedIndexIT.class.getSimpleName();

    private final RuledConnection conn = new RuledConnection(connPool,false);

    private final TableRule table =new TableRule(conn,"T","(C INTEGER, LCK_NUM SMALLINT)");
    @Rule public TestRule ruleChain =RuleChain.outerRule(conn)
            .around(new SchemaRule(conn,SCHEMA))
            .around(table);

    @Test
    public void testCanUpdateUsingPreparedStatements() throws Exception{
        //Regression test for DB-5427

        try(Statement s = conn.createStatement()){
            s.executeUpdate("create index idx3_T on "+table+"(C)");
        }

        try(PreparedStatement in1 = conn.prepareStatement("insert into "+table+" values (?,?)");
            PreparedStatement up1 = conn.prepareStatement("update "+table+" set LCK_NUM = LCK_NUM+1 where C = ?")){

            up1.setInt(1,12123);
            int updateCount = up1.executeUpdate();
            Assert.assertEquals("Incorrect number of rows updated!",0,updateCount);

            in1.setInt(1,11125);
            in1.setInt(2,1);
            int insertCount = in1.executeUpdate();
            Assert.assertEquals("Incorrect number of rows inserted!",1,insertCount);

            up1.setInt(1,11125);
            updateCount = up1.executeUpdate();
            Assert.assertEquals("Incorrect number of rows updated!",1,updateCount);
        }

    }
}
