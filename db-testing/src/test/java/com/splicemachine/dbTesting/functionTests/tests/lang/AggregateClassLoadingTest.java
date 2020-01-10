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

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.SecurityManagerSetup;

/**
 * Test for ensuring the aggregate implementation classes are loaded correctly,
 * even when the context class loader loads Derby engine classes as well. This
 * is a typical situation we have seen with J2EE servers where Derby may be in
 * the application WAR and provided as a system service by the container. <BR>
 * Jira issue DERBY-997 <BR>
 * Assumes embedded and only needs to be run in embedded, since all class
 * loading happens on the engine side.
 */
public class AggregateClassLoadingTest extends BaseJDBCTestCase {
	
	/**
	 * Basic constructor.
	 */	
	public AggregateClassLoadingTest(String name) {
		super(name);
	}
	
	/**
	 * Sets the auto commit to false.
	 */
	protected void initializeConnection(Connection conn) throws SQLException {
		conn.setAutoCommit(false);
	}
	
	/**
	 * Returns the implemented tests.
	 * 
	 * @return An instance of <code>Test</code> with the implemented tests to
	 *         run.
	 */
	public static Test suite() {
		
		
		/* this test creates a class loader, adding that permission to
		 * derbyTesting.jar would mean that permission was granted all
		 * the way up the stack to the db engine. Thus increasing
		 * the chance that incorrectly a privileged block could be dropped
		 * but the tests continue to pass. 
		 */		
		return SecurityManagerSetup.noSecurityManager(
						new CleanDatabaseTestSetup(
								new TestSuite(AggregateClassLoadingTest.class,
										"AggregateClassLoadingTest")) {
                            
                            /**
                             * Save the class loader upon entry to the
                             * suite, some JVM's install the main loader
                             * as the context loader.
                             */
                            private ClassLoader originalLoader;
                            protected void setUp() throws Exception {                    
                                originalLoader = Thread.currentThread().getContextClassLoader();
                                super.setUp();
                            }
							protected void tearDown() throws Exception {
								Thread.currentThread().setContextClassLoader(originalLoader);
								super.tearDown();
							}

							/**
							 * @see com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup#decorateSQL(java.sql.Statement)
							 */
							protected void decorateSQL(Statement s)
									throws SQLException {
								s.execute("create table t (i int)");
								s.execute("insert into t values 1,2,3,4,5,6,null,4,5,456,2,4,6,7,2144,44,2,-2,4");

								/*
								 * Find the location of the code for the Derby
								 * connection. The rest of the engine will be at
								 * the same location!
								 */
								URL derbyURL = s.getConnection().getClass().getProtectionDomain().getCodeSource()
										.getLocation();

								/*
								 * Create a new loader that loads from the same
								 * location as the engine. Create it without a
								 * parent, otherwise the parent will be the
								 * class loader of this class which is most
								 * likely the same as the engine. Since the
								 * class loader delegates to its parent first
								 * the bug would not show, as all the db
								 * engine classes would be from a single loader.
								 */
                                URLClassLoader cl = new URLClassLoader(new URL[] { derbyURL }, null);
								Thread.currentThread().setContextClassLoader(cl);

								super.decorateSQL(s);
							}
						});
		
	}		
		
	public void testAggregateMAX() throws SQLException {
		testAggregate("select MAX(i) from t");
	}
	
	public void testAggregateMIN() throws SQLException {
		testAggregate("select MIN(i) from t");
	}
	
	public void testAggregateAVG() throws SQLException {
		testAggregate("select AVG(i) from t");
	}
		
	public void testAggregateCOUNT() throws SQLException {
		testAggregate("select COUNT(i) from t");
	}
	
	public void testAggregateCOUNT2() throws SQLException {
		testAggregate("select COUNT(*) from t");
	}
	
    /**
     * Just run and display the aggregates result.
     * 
     * Test some aggregates, their generated class will attempt
	 * to load the internal aggregate through the context loader
	 * first, and then any remaining loader.
     */
    private void testAggregate(String query) throws SQLException {
		Statement s = createStatement();
        
        JDBC.assertDrainResults(s.executeQuery(query), 1);

        s.close();
   }	
}
