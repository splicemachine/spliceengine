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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test unique constraint
 */
public class UniqueConstraintSetNullTest extends BaseJDBCTestCase {
    
    /**
     * Basic constructor.
     */
    public UniqueConstraintSetNullTest(String name) {
        super(name);
    }
    
    /**
     * Returns the implemented tests.
     *
     * @return An instance of <code>Test</code> with the
     *         implemented tests to run.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("UniqueConstraintSetNullTest");
        suite.addTest(TestConfiguration.embeddedSuite(
                UniqueConstraintSetNullTest.class));
        return suite;
    }
    
    /**
     * Create table for test cases to use.
     */
    protected void setUp() throws Exception {
        Statement stmt = createStatement();
        stmt.executeUpdate("create table constraintest (" +
                "val1 varchar (20) not null, " +
                "val2 varchar (20))");
    }
    
    protected void tearDown() throws Exception {
        dropTable("constraintest");
        commit();
        super.tearDown();
    }
    /**
     * Test the behaviour of unique constraint after making
     * column nullable.
     * @throws java.lang.Exception
     */
    public void testUpdateNullablity() throws Exception {
        Statement stmt = createStatement();
        //create constraint
        stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1)");
        //test the constraint without setting it to nullable
        PreparedStatement ps = prepareStatement("insert into " +
                "constraintest (val1) values (?)");
        ps.setString (1, "name1");
        ps.executeUpdate();
        try {
            ps.setString (1, "name1");
            ps.execute();
            fail ("duplicate key in unique constraint!!!");
        }
        catch (SQLException e){
            assertSQLState ("duplicate key in unique constraint",
                    "23505", e);
        }
        try {
            ps.setNull(1, Types.VARCHAR);
            ps.executeUpdate();
            fail ("null value in not null field!!");
        }
        catch (SQLException e){
            assertSQLState ("null value in non null field",
                    "23502", e);
        }
        stmt.executeUpdate("alter table constraintest alter column val1 null");
        //should work
        ps.setNull(1, Types.VARCHAR);
        ps.executeUpdate();
        //try another null
        ps.setNull(1, Types.VARCHAR);
        ps.executeUpdate();
        //try a duplicate non null should fail
        try {
            ps.setString (1, "name1");
            ps.execute();
            fail ("duplicate key in unique constraint!!!");
        }
        catch (SQLException e){
            assertSQLState ("duplicate key in unique constraint",
                    "23505", e);
        }
        //remove nulls from table and set the column back to non null
        stmt.executeUpdate("delete from constraintest where val1 is null");
        stmt.executeUpdate("alter table constraintest alter column " +
                "val1 not null");
        //try a duplicate non null key
        try {
            ps.setString (1, "name1");
            ps.execute();
            fail ("duplicate key in unique constraint!!!");
        }
        catch (SQLException e){
            assertSQLState ("duplicate key in unique constraint",
                    "23505", e);
        }
        try {
            ps.setNull(1, Types.VARCHAR);
            ps.executeUpdate();
            fail ("null value in not null field!!");
        }
        catch (SQLException e){
            assertSQLState ("null value in non null field",
                    "23502", e);
        }
    }
}
