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

package com.splicemachine.dbTesting.functionTests.tests.store;

import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.sql.*;
import java.security.AccessController;
import java.security.CodeSource;
import java.util.Properties;

import javax.sql.DataSource;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.extensions.TestSetup;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.JDBCDataSource;
import com.splicemachine.dbTesting.junit.SecurityManagerSetup;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;


/*
 * This class tests a database boots using  class loaders. Test cases in this
 * class checks only one instance of a database can exist evenif database is 
 * booted using different class loader instances.    
 */
public class ClassLoaderBootTest extends BaseJDBCTestCase {

    private static URL derbyClassLocation; 
	static {
        // find the location of db jar file or location
        // of classes. 
        CodeSource cs;
        try {
            Class cls = Class.forName("com.splicemachine.db.database.Database");
            cs = cls.getProtectionDomain().getCodeSource();
        } catch (ClassNotFoundException e) {
            cs = null;
        }

        if(cs == null )
            derbyClassLocation = null;        
        else 
            derbyClassLocation = cs.getLocation();
	}
        

    private ClassLoader loader_1;
    private ClassLoader loader_2;
    private ClassLoader mainLoader;


    public ClassLoaderBootTest(String name ) {
        super(name);
    }

    /**
     * Runs the tests in the default embedded configuration and then
     * the client server configuration.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite(ClassLoaderBootTest.class);
        Test test = suite;
        TestSetup setup = 
            new CleanDatabaseTestSetup(test) {
                 protected void setUp() throws Exception {
                     super.setUp();
                     //shutdown the database. 
                     DataSource ds = JDBCDataSource.getDataSource();
                     JDBCDataSource.shutdownDatabase(ds);
                 }
            };
        Properties p = new Properties();
        p.setProperty("derby.infolog.append", "true");
                                   
        setup = new SystemPropertyTestSetup(setup,p);
        return SecurityManagerSetup.noSecurityManager(setup);
    }


    /**
     * Simple set up, just setup the loaders.
     * @throws SQLException 
     */
    protected void setUp() throws Exception
    {
        URL[] urls = new URL[]{derbyClassLocation};
        mainLoader = java.lang.Thread.currentThread().getContextClassLoader();

        loader_1 = createDerbyClassLoader(urls);
        loader_2 = createDerbyClassLoader(urls);
    }

    protected void    tearDown()
        throws Exception
    {
        if ( mainLoader != null ) { setThreadLoader(mainLoader); }

        loader_1 = null;
        loader_2 = null;
        mainLoader = null;
    }


    /**
     * Given a loaded class, this
     * routine asks the class's class loader for information about where the
     * class was loaded from. Typically, this is a file, which might be
     * either a class file or a jar file. The routine figures that out, and
     * returns the name of the file. If it can't figure it out, it returns null
     */
    private DerbyURLClassLoader createDerbyClassLoader(final URL[] urls) 
        throws Exception 
    {
        return (DerbyURLClassLoader)AccessController.doPrivileged
            (
             new java.security.PrivilegedExceptionAction(){   
                 public Object run()
                 {
                     return new DerbyURLClassLoader(urls);
                 }
             });
    }


    
    /* 
     * Test booting a database, that was alreadt booted by another class loader.
     */
	public void testBootingAnAlreadyBootedDatabase() throws SQLException 
    {
        //
        // This test relies on a bug fix in Java 6. Java 5 does not have this
        // bug fix and will fail this test. See DERBY-700.
        //
        if (!JDBC.vmSupportsJDBC4())
        {
            println( "The dual boot test only runs on Java 6 and higher." );
            return;
        }

        println( "The dual boot test is running." );
        
        // first boot the database using one loader and attempt 
        // to boot it using another loader, it should fail to boot.

        setThreadLoader(loader_1);
        DataSource ds_1 = JDBCDataSource.getDataSource();
        Connection conn1 = ds_1.getConnection();
        // now attemp to boot using another class loader.
        setThreadLoader(loader_2);
        DataSource ds_2 = JDBCDataSource.getDataSource();
        try {
            ds_2.getConnection();
            fail("booted database that was already booted by another CLR");
        } catch (SQLException e) {
            SQLException ne = e.getNextException();
            assertPreventDualBoot(ne);
            JDBCDataSource.shutEngine(ds_2);
        }
        
        // shutdown the engine.
        setThreadLoader(loader_1);
        JDBCDataSource.shutEngine(ds_1);
    }

    
    /* 
     * Test booting a database, that was  booted and shutdown 
     * by another class loader.
     */
	public void testBootingDatabaseShutdownByAnotherCLR() throws SQLException 
    {
        // first boot the database using one loader and shutdown and then 
        // attempt to boot it using another loader, it should boot.

        setThreadLoader(loader_1);
        DataSource ds_1 = JDBCDataSource.getDataSource();
        Connection conn1 = ds_1.getConnection();
        //shutdown the database.
        JDBCDataSource.shutdownDatabase(ds_1);
        // now attemp to boot using another class loader.
        setThreadLoader(loader_2);
        DataSource ds_2 = JDBCDataSource.getDataSource();
        ds_2.getConnection();
        // shutdown the engine for both the class loaders.
        JDBCDataSource.shutEngine(ds_2);
        JDBCDataSource.shutEngine(ds_1);
}

    private void setThreadLoader(final ClassLoader which) {

        AccessController.doPrivileged
        (new java.security.PrivilegedAction(){
            
            public Object run()  { 
                java.lang.Thread.currentThread().setContextClassLoader(which);
              return null;
            }
        });
    }


	private static void assertPreventDualBoot(SQLException ne) {
		assertNotNull(ne);
		String state = ne.getSQLState();
		assertTrue("Unexpected SQLState:" + state, state.equals("XSDB6"));
	}



    /*
     * Simple specialized URLClassLoader for Derby.  
     * Filters all db classes out of parent ClassLoader to ensure
     * that Derby classes are loaded from the URL specified
     */
    public class DerbyURLClassLoader extends URLClassLoader {
	
        /**
         * @see java.net.URLClassLoader#URLClassLoader(URL[] urls)
         */
        public DerbyURLClassLoader(URL[] urls) {
            super(urls);
        }


        /**
         * @see java.net.URLClassLoader#URLClassLoader(URL[] urls, 
         *      ClassLoader parent)
         */
        public DerbyURLClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
	
        }
	
        /**
         *@see java.net.URLClassLoader#URLClassLoader(java.net.URL[], 
         *      java.lang.ClassLoader, java.net.URLStreamHandlerFactory)
         */
        public DerbyURLClassLoader(URL[] urls, ClassLoader parent,
                                   URLStreamHandlerFactory factory) {
            super(urls, parent, factory);
		
        }
	
        /* Override the parent class loader to filter out any db
         * jars in the classpath.  Any classes that start with 
         * "com.splicemachine.db" will load  from the URLClassLoader
         * 
         * @see java.lang.ClassLoader#loadClass(java.lang.String, boolean)
         */
        protected synchronized Class loadClass(String name, boolean resolve)
            throws ClassNotFoundException
        {

            Class cl = findLoadedClass(name);
            if (cl == null) {
                // cut off delegation to parent for certain classes
                // to ensure loading from the desired source
                if (!name.startsWith("com.splicemachine.db")) {
                    cl = getParent().loadClass(name);
		    	}
		    }
            if (cl == null) cl = findClass(name);
            if (cl == null) throw new ClassNotFoundException();
            if (resolve) resolveClass(cl);
            return cl;
        }

        /* 
         * @see java.lang.ClassLoader#loadClass(java.lang.String)
         */
        public Class loadClass(String name) throws ClassNotFoundException {
                return loadClass(name, false);
        }

    }
}

