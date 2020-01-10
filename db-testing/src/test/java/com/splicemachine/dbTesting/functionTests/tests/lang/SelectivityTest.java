/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;

public class SelectivityTest extends BaseJDBCTestCase {

    public SelectivityTest(String name) {
        super(name);
    }
    
    public void testSingleColumnSelectivity() throws SQLException {
        // choose whatever plan you want but the row estimate should be.
        //(n * n) * 0.5
        Connection conn = getConnection();
        Statement s = createStatement();      
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        s.executeQuery("select template.id from --DERBY-PROPERTIES joinOrder=fixed\n" 
                + "test, template where test.two = template.two").close();         
//        checkEstimatedRowCount(conn,8020012.5);
        

            
    }
    
    public static Test suite() {
        return new CleanDatabaseTestSetup(new TestSuite(SelectivityTest.class,
                "SelectivityTest")) {
            protected void decorateSQL(Statement s) throws SQLException {
                s.executeUpdate("create table two (x int)");
                s.executeUpdate("insert into two values (1),(2)");
                s.executeUpdate("create table ten (x int)");
                s
                        .executeUpdate("insert into ten values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)");
                s.executeUpdate("create table twenty (x int)");
                s
                        .executeUpdate("insert into twenty values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20)");
                s
                        .executeUpdate("create table hundred (x int generated always as identity, dc int)");
                s
                        .executeUpdate("insert into hundred (dc) select t1.x from ten t1, ten t2");
                s
                        .executeUpdate("create table template (id int not null generated always as identity, two int, twenty int, hundred int)");
                // 4000 rows
                s
                        .executeUpdate("insert into template (two, twenty, hundred) select two.x, twenty.x, hundred.x from two, twenty, hundred");
                s.executeUpdate("create index template_two on template(two)");
                s
                        .executeUpdate("create index template_twenty on template(twenty)");
                // 20 distinct values
                s
                        .executeUpdate("create index template_22 on template(twenty,two)");
                s
                        .executeUpdate("create unique index template_id on template(id)");
                s
                        .executeUpdate("create index template_102 on template(hundred,two)");
                s
                        .executeUpdate("create table test (id int, two int, twenty int, hundred int)");
                s.executeUpdate("create index test_id on test(id)");
                s.executeUpdate("insert into test select * from template");

                s
                        .executeUpdate("create view showstats as "
                                + "select cast (conglomeratename as varchar(20)) indexname, "
                                + "cast (statistics as varchar(40)) stats, "
                                + "creationtimestamp createtime, "
                                + "colcount ncols "
                                + "from sys.sysstatistics, sys.sysconglomerates "
                                + "where conglomerateid = referenceid");
                ResultSet statsrs = s
                        .executeQuery("select indexname, stats, ncols from showstats order by indexname, stats, createtime, ncols");
                JDBC.assertFullResultSet(statsrs, new String[][] {
                        {"TEMPLATE_102","numunique= 100 numrows= 4000","1"},
                        {"TEMPLATE_102","numunique= 200 numrows= 4000","2"},
                        {"TEMPLATE_22","numunique= 20 numrows= 4000","1"},
                        {"TEMPLATE_22","numunique= 40 numrows= 4000","2"},
                        {"TEMPLATE_TWENTY","numunique= 20 numrows= 4000","1"},
                        {"TEMPLATE_TWO","numunique= 2 numrows= 4000","1"}});               
                s
                        .executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('SPLICE','TEMPLATE',null)");
                s
                        .executeUpdate("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('SPLICE','TEST',null)");
                statsrs = s
                        .executeQuery("select  indexname, stats, ncols from showstats order by indexname, stats, createtime, ncols");
                JDBC.assertFullResultSet(statsrs, new String[][] {
                        {"TEMPLATE_102","numunique= 100 numrows= 4000","1"},
                        {"TEMPLATE_102","numunique= 200 numrows= 4000","2"},
                        {"TEMPLATE_22","numunique= 20 numrows= 4000","1"},
                        {"TEMPLATE_22","numunique= 40 numrows= 4000","2"},
                        {"TEMPLATE_TWENTY","numunique= 20 numrows= 4000","1"},
                        {"TEMPLATE_TWO","numunique= 2 numrows= 4000","1"},
                        {"TEST_ID","numunique= 4000 numrows= 4000","1"}}                                                               
                );
                
            }
        };
    }
}
