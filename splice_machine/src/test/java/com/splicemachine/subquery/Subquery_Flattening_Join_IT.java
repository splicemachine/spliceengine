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

package com.splicemachine.subquery;

import com.splicemachine.derby.test.framework.RuledConnection;
import com.splicemachine.derby.test.framework.SchemaRule;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.TableRule;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Tests for joins over subquery operations.
 *
 * @author Scott Fines
 *         Date: 7/25/16
 */
public class Subquery_Flattening_Join_IT {

    private static final String SCHEMA = Subquery_Flattening_Join_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    public RuledConnection conn = new RuledConnection(null,true);

    public SchemaRule schema = new SchemaRule(conn, SCHEMA.toUpperCase());

    public TableRule a = new TableRule(conn,"A","(account_id varchar(75),first_name varchar(25),last_name varchar(25))");
    public TableRule b = new TableRule(conn,"B","(account_id bigint,trans_amt DECIMAL(10,2))");

    @Rule public TestRule ruleChain = RuleChain.outerRule(conn)
            .around(schema)
            .around(a)
            .around(b);

    @Test
    public void testFlattensSubqueryJoinCorrectly() throws Exception{
        /*
         * Regression test for SPLICE-712. Determines that the query as written properly compiles and contains
         * no subquery nodes
         */
        try(Statement s=conn.createStatement()){
            try(ResultSet rs=s.executeQuery("explain SELECT X.FIRST_NAME, X.LAST_NAME, B.TRANS_AMT FROM "+
                    "( SELECT A.FIRST_NAME, A.LAST_NAME, CAST (A.ACCOUNT_ID AS BIGINT) AS ACCOUNT_ID FROM A ) X "+
                    "JOIN B on X.ACCOUNT_ID = B.ACCOUNT_ID")){
                boolean hasRow=false;
                while(rs.next()){
                    hasRow=true;
                    String entry=rs.getString(1);
                    Assert.assertFalse("Returned null incorrectly!",rs.wasNull());
                    Assert.assertFalse("Contains a subquery node!",entry.contains("Subquery"));
                }
                Assert.assertTrue("Did not return results in explain!",hasRow);
            }
        }

    }
}
