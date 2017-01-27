/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.RuledConnection;
import com.splicemachine.derby.test.framework.SchemaRule;
import com.splicemachine.derby.test.framework.TableRule;
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
    private static final String SCHEMA = PreparedIndexIT.class.getSimpleName();

    private final RuledConnection conn = new RuledConnection(null,false);

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
