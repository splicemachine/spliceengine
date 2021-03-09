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

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.ivy.plugins.repository.ssh.AbstractSshBasedRepository;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Tests for Views
 * @author Scott Fines
 * Created on: 6/25/13
 */
public class ViewIT { 
    private static final Logger LOG = Logger.getLogger(ViewIT.class);

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(ViewIT.class.getSimpleName());

    protected static SpliceTableWatcher baseTableWatcher = new SpliceTableWatcher("t1",schemaWatcher.schemaName,"(a int, b int)");
    protected static SpliceTableWatcher tableWatcher = new SpliceTableWatcher("VIEW_IT_TABLE",schemaWatcher.schemaName,"(a int, b int)");

    private static int size = 10;
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(baseTableWatcher)
            .around(tableWatcher)
            .around(new SpliceDataWatcher(){

                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into "+ baseTableWatcher+" (a, b) values (?,?)");
                        for(int i=0;i<size;i++){
                            ps.setInt(1,i);
                            ps.setInt(2,i*2);
                            ps.executeUpdate();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCanUseView() throws Exception {


    }

    @Test
    public void testCanCreateViewWithLimitAndGroupedBy() throws Exception {
        // Regression test for Bug 579
        methodWatcher.prepareStatement("create view t1_view (a,b) as select a, count(b) from "+ baseTableWatcher +" group by a").executeUpdate();
        try{
            ResultSet rs = methodWatcher.executeQuery("select * from t1_view {limit 1}");
            int count=0;
            while(rs.next()){
                System.out.println(rs.getInt(1)+"+"+rs.getInt(2));
                count++;
            }
            Assert.assertEquals(1,count);
        }finally{
            methodWatcher.prepareStatement("drop view t1_view").execute();
        }
    }

    @Test
    public void testCreateViewInSessionSchema() throws Exception {
        try {
            methodWatcher.executeUpdate("create view session.vw1 as select * from " + baseTableWatcher);
            Assert.fail("expect failure to create a view in SESSION schema");
        } catch (SQLException e) {
            Assert.assertEquals("XCL51", e.getSQLState());
        }
    }

    @Test
    public void testSYSCONGLOMERATESVIEW() throws Exception {
        methodWatcher.execute("CREATE INDEX ViewIT.VIEW_IT_TABLE_IDX ON ViewIT.VIEW_IT_TABLE(a)");
        try ( ResultSet rs = methodWatcher.executeQuery("select * from SYSVW.SYSCONGLOMERATESVIEW WHERE CONGLOMERATENAME = 'VIEW_IT_TABLE_IDX'")) {
            String res = TestUtils.FormattedResult.ResultFactory.toString(rs);
            String expected =
                    "SCHEMAID§|§TABLEID§|CONGLOMERATENUMBER |CONGLOMERATENAME  | ISINDEX |DESCRIPTOR |ISCONSTRAINT |§CONGLOMERATEID§|\n" +
                    "---------------§--------------------------------------------\n" +
                    "§|§|§|VIEW_IT_TABLE_IDX |§true§| BTREE (1) |    false    |§|";
            SpliceUnitTest.matchMultipleLines(res, SpliceUnitTest.escapeRegexp(expected) );
        }
        methodWatcher.execute("DROP INDEX ViewIT.VIEW_IT_TABLE_IDX");
    }

    @Test
    public void testSYSSEQUENCESVIEW() throws Exception {
        methodWatcher.execute("CREATE SEQUENCE ViewIT_SEQUENCE AS BIGINT START WITH 3000000000");
        try ( ResultSet rs = methodWatcher.executeQuery("select * from SYSVW.SYSSEQUENCESVIEW WHERE SEQUENCENAME = 'VIEWIT_SEQUENCE'")) {
            String res = TestUtils.FormattedResult.ResultFactory.toString(rs);
            String expected =
                    "SEQUENCEID§|§SEQUENCENAME§|§SCHEMAID§|SEQUENCEDATATYPE |CURRENTVALUE |STARTVALUE |    MINIMUMVALUE     |   MAXIMUMVALUE     | INCREMENT | CYCLEOPTION |\n" +
                    "------------------§-------------------------------------------------------------------------------------------------------------------\n" +
                    "§|VIEWIT_SEQUENCE§|§|     BIGINT      | 3000000000  |3000000000 |-9223372036854775808 |9223372036854775807 |     1     |      N      |\n";
            SpliceUnitTest.matchMultipleLines(res, SpliceUnitTest.escapeRegexp(expected) );
        }
        finally {
            methodWatcher.execute("DROP SEQUENCE ViewIT_SEQUENCE RESTRICT");
        }

    }
}
