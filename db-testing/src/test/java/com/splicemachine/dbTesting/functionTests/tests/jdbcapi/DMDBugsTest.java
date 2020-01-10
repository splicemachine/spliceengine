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
package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

public class DMDBugsTest extends BaseJDBCTestCase {

	public DMDBugsTest(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	
	public void testDerby3000() throws SQLException, IOException {
		ResultSet rs;
		// Derby-3000 make sure we process only valid TableType values and
		// process them correctly.
		DatabaseMetaData dmd = getConnection().getMetaData();
	
		Statement s = createStatement();
		s.executeUpdate("CREATE TABLE SPLICE.TAB (i int)");
		s.executeUpdate("CREATE VIEW  SPLICE.V  as SELECT * FROM TAB");
		s.executeUpdate("CREATE SYNONYM TSYN FOR SPLICE.TAB");
	
		String[] withInvalidTableTypes = {"SYNONYM","TABLE","VIEW",
		"GLOBAL TEMPORARY"};
		// just ignore invalid types
		rs = dmd.getTables( "%", "%", "%", withInvalidTableTypes);			
		JDBC.assertFullResultSet(rs,
			new String[][] {{"","SPLICE","TSYN","SYNONYM","",null,null,null,null,null},
			{"","SPLICE","TAB","TABLE","",null,null,null,null,null},
			{"","SPLICE","V","VIEW","",null,null,null,null,null}});


		rs = dmd.getTables("%", "%", "%", new String[] {"GLOBAL TEMPORARY"});
		JDBC.assertEmpty(rs);
		
		rs = dmd.getTables("%", "%", "%", new String[] {"VIEW"});
		JDBC.assertUnorderedResultSet(rs, new String[][] 
		            {{"","SPLICE","V","VIEW","",null,null,null,null,null}});

		
		rs = dmd.getTables("%", "%", "%", new String[] {"TABLE"});
		JDBC.assertUnorderedResultSet(rs,new String[][]
		          {{"","SPLICE","TAB","TABLE","",null,null,null,null,null}} );
		
		rs = dmd.getTables("%", "%", "%", new String[] {"SYNONYM"});
		JDBC.assertUnorderedResultSet(rs, new String[][]
	                  {{"","SPLICE","TSYN","SYNONYM","",null,null,null,null,null}});

		rs = dmd.getTables( "%", "%", "%", new String[] {"SYSTEM TABLE"});
		assertEquals(23, JDBC.assertDrainResults(rs));
		s.executeUpdate("DROP VIEW SPLICE.V");
		s.executeUpdate("DROP TABLE SPLICE.TAB");
		s.executeUpdate("DROP SYNONYM SPLICE.TSYN");
	}
	   
		
	/* Default suite for running this test.
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite("DMDBugsTest");
		suite.addTest(
				TestConfiguration.defaultSuite(DMDBugsTest.class));
		return suite;
	        	
	}
}
