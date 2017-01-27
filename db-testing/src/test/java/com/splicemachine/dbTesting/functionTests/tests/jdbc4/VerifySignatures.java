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

package com.splicemachine.dbTesting.functionTests.tests.jdbc4;

import java.sql.*;
import javax.sql.*;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import junit.framework.Test;
import com.splicemachine.dbTesting.functionTests.util.TestUtil;
import com.splicemachine.dbTesting.junit.BaseTestCase;
import com.splicemachine.dbTesting.junit.J2EEDataSource;
import com.splicemachine.dbTesting.junit.JDBCDataSource;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * JUnit test which checks that all methods specified by the
 * interfaces in JDBC 4.0 are implemented. The test requires JVM 1.6
 * to run.
 * Even though this class uses JDBC it extends BaseTestCase as
 * it handles getting connections itself and thus does not
 * need the utility methods or connecion handlng provided by
 * BaseJDBCTestCase.
 */
public class VerifySignatures extends BaseTestCase {

    /**
     * All the java.sql and javax.sql interfaces specified by JDBC 4.0.
     */
    private final static Class[] JDBC_INTERFACES = {
        java.sql.Array.class,
        java.sql.Blob.class,
        java.sql.CallableStatement.class,
        java.sql.Clob.class,
        java.sql.Connection.class,
        java.sql.DatabaseMetaData.class,
        java.sql.Driver.class,
        java.sql.NClob.class,
        java.sql.ParameterMetaData.class,
        java.sql.PreparedStatement.class,
        java.sql.Ref.class,
        java.sql.ResultSet.class,
        java.sql.ResultSetMetaData.class,
        java.sql.RowId.class,
        java.sql.Savepoint.class,
        java.sql.SQLData.class,
        java.sql.SQLInput.class,
        java.sql.SQLOutput.class,
        java.sql.SQLXML.class,
        java.sql.Statement.class,
        java.sql.Struct.class,
        java.sql.Wrapper.class,
        javax.sql.CommonDataSource.class,
        javax.sql.ConnectionEventListener.class,
        javax.sql.ConnectionPoolDataSource.class,
        javax.sql.DataSource.class,
        javax.sql.PooledConnection.class,
        javax.sql.RowSet.class,
        javax.sql.RowSetInternal.class,
        javax.sql.RowSetListener.class,
        javax.sql.RowSetMetaData.class,
        javax.sql.RowSetReader.class,
        javax.sql.RowSetWriter.class,
        javax.sql.StatementEventListener.class,
        javax.sql.XAConnection.class,
        javax.sql.XADataSource.class,
    };

    /**
     * Creates a new instance.
     */
    public VerifySignatures(String name) {
        super(name);
    }

    /**
     * Build a suite of tests to be run.
     *
     * @return a test suite
     */
    public static Test suite()  {
        return TestConfiguration.defaultSuite(VerifySignatures.class);
    }
    
    public void testAllJDBCObjects()
      throws NoSuchMethodException, SQLException
    {
        // set of all implementation/interface pairs found
        Set<ClassInfo> classes = new HashSet<ClassInfo>();

        collectClassesFromDataSource(classes);
        collectClassesFromConnectionPoolDataSource(classes);
        collectClassesFromXADataSource(classes);
        addClass(classes,
                 DriverManager.getDriver(TestConfiguration.getCurrent().getJDBCUrl()).getClass(),
                 java.sql.Driver.class);

        // all interfaces for which tests have been generated
        Set<Class> interfaces = new HashSet<Class>();

        for (ClassInfo pair : classes) {
            // some methods are defined in many interfaces, so collect
            // them in a set first to avoid duplicates
            Set<Method> methods = new HashSet<Method>();
            for (Class iface : getAllInterfaces(pair.jdbcInterface)) {
                interfaces.add(iface);
                for (Method method : iface.getMethods()) {
                    methods.add(method);
                }
            }
            for (Method method : methods) {
                checkImplementationMethod(pair.derbyImplementation,
                        method);
            }
        }
        
        // Now ensure all interfaces were covered.
        // get the declared set of interfaces Derby is
        // expected to implement.
        Set<Class> jdbcInterfaces = getInterfacesToCheck();
        
        // remove from it all that were tested.
        jdbcInterfaces.removeAll(interfaces);
        
        // and the resultin set should be empty if we tested all!
        assertTrue("Unchecked interfaces: " + jdbcInterfaces,
                   jdbcInterfaces.isEmpty());
    }

    /**
     * Obtain a connection from a <code>DataSource</code> object and
     * perform JDBC operations on it. Collect the classes of all JDBC
     * objects that are found.
     *
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void collectClassesFromDataSource(Set<ClassInfo> classes)
        throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                                       "create=true");
        addClass(classes, ds.getClass(), javax.sql.DataSource.class);
        collectClassesFromConnection(ds.getConnection
                                     (TestConfiguration.getCurrent().getUserName(),
                                             TestConfiguration.getCurrent().getUserPassword()),
                                     classes);
    }

    /**
     * Obtain a connection from a <code>ConnectionPoolDataSource</code>
     * object and perform JDBC operations on it. Collect the classes
     * of all JDBC objects that are found.
     *
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void
        collectClassesFromConnectionPoolDataSource(Set<ClassInfo> classes)
        throws SQLException
    {
        ConnectionPoolDataSource cpds = J2EEDataSource.getConnectionPoolDataSource();
        addClass(classes,
                 cpds.getClass(), javax.sql.ConnectionPoolDataSource.class);

        PooledConnection pc =
            cpds.getPooledConnection(TestConfiguration.getCurrent().getUserName(),
                    TestConfiguration.getCurrent().getUserPassword());
        addClass(classes, pc.getClass(), javax.sql.PooledConnection.class);

        collectClassesFromConnection(pc.getConnection(), classes);

        pc.close();
    }

    /**
     * Obtain a connection from an <code>XADataSource</code> object
     * and perform JDBC operations on it. Collect the classes of all
     * JDBC objects that are found.
     *
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void collectClassesFromXADataSource(Set<ClassInfo> classes)
        throws SQLException
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        addClass(classes, xads.getClass(), javax.sql.XADataSource.class);

        XAConnection xaconn = xads.getXAConnection(TestConfiguration.getCurrent().getUserName(),
                TestConfiguration.getCurrent().getUserPassword());
        addClass(classes, xaconn.getClass(), javax.sql.XAConnection.class);

        collectClassesFromConnection(xaconn.getConnection(), classes);
    }

    /**
     * Perform JDBC operations on a <code>Connection</code>. Collect
     * the classes of all JDBC objects that are found.
     *
     * @param conn connection to a database
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void collectClassesFromConnection(Connection conn,
                                                     Set<ClassInfo> classes)
        throws SQLException
    {
        conn.setAutoCommit(false);
        addClass(classes, conn.getClass(), java.sql.Connection.class);

        Savepoint sp = conn.setSavepoint();
        addClass(classes, sp.getClass(), java.sql.Savepoint.class);
        conn.releaseSavepoint(sp);

        DatabaseMetaData dmd = conn.getMetaData();
        addClass(classes, dmd.getClass(), java.sql.DatabaseMetaData.class);

        collectClassesFromStatement(conn, classes);
        collectClassesFromPreparedStatement(conn, classes);
        collectClassesFromCallableStatement(conn, classes);
        conn.rollback();
        conn.close();
    }

    /**
     * Perform JDBC operations on a <code>Statement</code>. Collect
     * the classes of all JDBC objects that are found.
     *
     * @param conn connection to a database
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void
        collectClassesFromStatement(Connection conn, Set<ClassInfo> classes)
        throws SQLException
    {
        Statement stmt = conn.createStatement();
        addClass(classes, stmt.getClass(), java.sql.Statement.class);

        stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, " +
                     "b BLOB(10), c CLOB(10))");
        stmt.execute("INSERT INTO t (id, b, c) VALUES (1, "+
                     "CAST (" + TestUtil.stringToHexLiteral("101010001101") +
                     "AS BLOB(10)), CAST ('hello' AS CLOB(10)))");

        ResultSet rs = stmt.executeQuery("SELECT id, b, c FROM t");
        addClass(classes, rs.getClass(), java.sql.ResultSet.class);
        rs.next();
        Blob b = rs.getBlob(2);
        addClass(classes, b.getClass(), java.sql.Blob.class);
        Clob c = rs.getClob(3);
        addClass(classes, c.getClass(), java.sql.Clob.class);
        ResultSetMetaData rsmd = rs.getMetaData();
        addClass(classes, rsmd.getClass(), java.sql.ResultSetMetaData.class);
        rs.close();

        stmt.close();
        conn.rollback();
    }

    /**
     * Perform JDBC operations on a <code>PreparedStatement</code>.
     * Collect the classes of all JDBC objects that are found.
     *
     * @param conn connection to a database
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void
        collectClassesFromPreparedStatement(Connection conn,
                                            Set<ClassInfo> classes)
        throws SQLException
    {
        PreparedStatement ps = conn.prepareStatement("VALUES(1)");
        addClass(classes, ps.getClass(), java.sql.PreparedStatement.class);
        ResultSet rs = ps.executeQuery();
        addClass(classes, rs.getClass(), java.sql.ResultSet.class);
        rs.close();

        ParameterMetaData pmd = ps.getParameterMetaData();
        addClass(classes, pmd.getClass(), java.sql.ParameterMetaData.class);

        ps.close();
    }

    /**
     * Perform JDBC operations on a <code>CallableStatement</code>.
     * Collect the classes of all JDBC objects that are found.
     *
     * @param conn connection to a database
     * @param classes set into which classes are collected
     * @exception SQLException if a database error occurs
     */
    private static void
        collectClassesFromCallableStatement(Connection conn,
                                            Set<ClassInfo> classes)
        throws SQLException
    {
        CallableStatement cs =
            conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
        addClass(classes, cs.getClass(), java.sql.CallableStatement.class);

        ParameterMetaData pmd = cs.getParameterMetaData();
        addClass(classes, pmd.getClass(), java.sql.ParameterMetaData.class);

        cs.close();
    }

    /**
     * Adds a <code>ClassInfo</code> object to a set.
     *
     * @param classes set to which the class should be added
     * @param implementation Derby implementation class
     * @param iface JDBC interface supposed to be implemented
     */
    private static void addClass(Set<ClassInfo> classes,
                                 Class implementation, Class iface) {
        classes.add(new ClassInfo(implementation, iface));
    }

    /**
     * Get the set consisting of an interface and all its
     * super-interfaces.
     *
     * @param iface an interface
     * @return the set consisting of <code>iface</code> and all its
     * super-interfaces
     */
    private static Set<Class> getAllInterfaces(Class iface) {
        Set<Class> set = new HashSet<Class>();
        set.add(iface);
        for (Class superIface : iface.getInterfaces()) {
            set.add(superIface);
            set.addAll(getAllInterfaces(superIface));
        }
        return set;
    }
   
    /**
     * checks that a class implements a specific method.
     * @param derbyImplementation The Derby implementation class which is tested
     * @param ifaceMethod The method that should be implemented.
     */
    private static void checkImplementationMethod(
            Class<?> derbyImplementation, Method ifaceMethod)
        throws NoSuchMethodException
    {
        
            assertFalse("Implementation class is interface",
                        derbyImplementation.isInterface());

            Method impMethod =
                derbyImplementation.getMethod(ifaceMethod.getName(),
                                              ifaceMethod.getParameterTypes());

            assertEquals("Incorrect return type",
                         ifaceMethod.getReturnType(),
                         impMethod.getReturnType());

            int modifiers = impMethod.getModifiers();
            assertTrue("Non-public method", Modifier.isPublic(modifiers));
            assertFalse("Abstract method", Modifier.isAbstract(modifiers));
            assertFalse("Static method", Modifier.isStatic(modifiers));

            Class[] declaredExceptions = ifaceMethod.getExceptionTypes();
            for (Class exception : impMethod.getExceptionTypes()) {
                if (RuntimeException.class.isAssignableFrom(exception)) {
                    continue;
                }
                assertNotNull("Incompatible throws clause",
                              findCompatibleClass(exception,
                                                  declaredExceptions));
            }
        }

        /**
         * Search an array of classes for a class that is identical to
         * or a super-class of the specified exception class.
         *
         * @param exception an exception class
         * @param declared an array of exception classes declared to
         * be thrown by a method
         * @return a class that is compatible with the specified
         * exception class, or <code>null</code> if no compatible
         * class is found
         */
        private static Class findCompatibleClass(Class exception, Class[] declared)
        {
            for (Class<?> dec : declared) {
                if (dec.isAssignableFrom(exception)) {
                    return dec;
                }
            }
            return null;
        }

        /**
         * Returns the declared set of JDBC interfaces that
         * Derby implements.
         *
         */
        private static Set<Class> getInterfacesToCheck() {

            Set<Class> jdbcInterfaces = new HashSet<Class>(Arrays.asList(JDBC_INTERFACES));

            // Remove the interfaces that we know we haven't checked.

            // Interfaces that Derby doesn't implement:
            jdbcInterfaces.remove(java.sql.Array.class);
            jdbcInterfaces.remove(java.sql.NClob.class);
            jdbcInterfaces.remove(java.sql.Ref.class);
            jdbcInterfaces.remove(java.sql.SQLData.class);
            jdbcInterfaces.remove(java.sql.SQLInput.class);
            jdbcInterfaces.remove(java.sql.SQLOutput.class);
            jdbcInterfaces.remove(java.sql.SQLXML.class);
            jdbcInterfaces.remove(java.sql.Struct.class);
            jdbcInterfaces.remove(javax.sql.RowSet.class);
            jdbcInterfaces.remove(javax.sql.RowSetInternal.class);
            jdbcInterfaces.remove(javax.sql.RowSetListener.class);
            jdbcInterfaces.remove(javax.sql.RowSetMetaData.class);
            jdbcInterfaces.remove(javax.sql.RowSetReader.class);
            jdbcInterfaces.remove(javax.sql.RowSetWriter.class);
            jdbcInterfaces.remove(java.sql.RowId.class);

            // The event listener interfaces are implemented in
            // application code, not in Derby code.
            jdbcInterfaces.remove(javax.sql.ConnectionEventListener.class);
            jdbcInterfaces.remove(javax.sql.StatementEventListener.class);
            
            return jdbcInterfaces;
        }

    /**
     * Data structure holding a Derby implementation class and the
     * JDBC interface it is supposed to implement.
     */
    private static class ClassInfo {
        /** Derby implementation class. */
        Class derbyImplementation;
        /** JDBC interface which should be implemented. */
        Class jdbcInterface;

        /**
         * Creates a new <code>ClassInfo</code> instance.
         *
         * @param imp the Derby implementation class
         * @param iface the JDBC interface
         */
        ClassInfo(Class imp, Class iface) {
            derbyImplementation = imp;
            jdbcInterface = iface;
        }

        /**
         * Checks whether this object is equal to another object.
         *
         * @param x another object
         * @return <code>true</code> if the objects are equal,
         * <code>false</code> otherwise
         */
        public boolean equals(Object x) {
            if (x instanceof ClassInfo) {
                ClassInfo ci = (ClassInfo) x;
                return
                    derbyImplementation.equals(ci.derbyImplementation) &&
                    jdbcInterface.equals(ci.jdbcInterface);
            }
            return false;
        }

        /**
         * Calculate hash code.
         *
         * @return hash code
         */
        public int hashCode() {
            return derbyImplementation.hashCode() ^ jdbcInterface.hashCode();
        }
    }
}
