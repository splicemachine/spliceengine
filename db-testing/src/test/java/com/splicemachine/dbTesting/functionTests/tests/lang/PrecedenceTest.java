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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test case for precedence.sql.It tests precedence of operators other than and,
 * or, and not that return boolean.
 */
public class PrecedenceTest extends BaseJDBCTestCase {

    public PrecedenceTest(String name) {
        super(name);
    }
    
    public static Test suite(){
        return TestConfiguration.defaultSuite(PrecedenceTest.class);
    }
    
    public void testPrecedence() throws SQLException{
    	String sql = "create table t1(c11 int)";
    	Statement st = createStatement();
    	st.executeUpdate(sql);
    	
    	sql = "insert into t1 values(1)";
    	assertEquals(1, st.executeUpdate(sql));
    	
    	sql = "select c11 from t1 where 1 in (1,2,3) = (1=1)";
    	JDBC.assertSingleValueResultSet(st.executeQuery(sql), "1");
    	
    	sql = "select c11 from t1 where 'acme widgets' " +
    			"like 'acme%' in ('1=1')";
    	JDBC.assertSingleValueResultSet(st.executeQuery(sql), "1");
    	
    	sql = "select c11 from t1 where 1 between -100 " +
    			"and 100 is not null";
    	JDBC.assertSingleValueResultSet(st.executeQuery(sql), "1");
    	
    	sql = "select c11 from t1 where exists(select *" +
    			" from (values 1) as t) not in ('1=2')";
    	JDBC.assertEmpty(st.executeQuery(sql));
    	
    	st.close();
    }
}
