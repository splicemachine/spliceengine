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
    
package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import javax.sql.XADataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.J2EEDataSource;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.JDBCDataSource;
import com.splicemachine.dbTesting.junit.TestConfiguration;

public class XAJNDITest extends BaseJDBCTestCase {
    private static String ldapServer;
    private static String ldapPort;
    private static String dnString;
    private InitialDirContext ic = getInitialDirContext();

    public XAJNDITest(String name) {
        super(name);
    }

    public static Test suite() {
        // the test requires XADataSource to run, so check for JDBC3 support
        if (JDBC.vmSupportsJDBC3()) {
            ldapServer=getSystemProperty("derbyTesting.ldapServer");
            if (ldapServer == null || ldapServer.length() < 1)
                return new TestSuite("XAJNDITest requires property derbyTesting.ldapServer set, eg: -DderbyTesting.ldapServer=myldapserver.myorg.org");
            ldapPort=getSystemProperty("derbyTesting.ldapPort");
            if (ldapPort == null || ldapPort.length() < 1)
                return new TestSuite("XAJNDITest requires property derbyTesting.ldapPort set, eg: -DderbyTesting.ldapPort=333");
            dnString=getSystemProperty("derbyTesting.dnString");
            if (dnString == null || dnString.length() < 1)
                return new TestSuite("XAJNDITest requires property derbyTesting.dnString for setting o=, eg: -DderbyTesting.dnString=myJNDIstring");
            
            return TestConfiguration.defaultSuite(XAJNDITest.class);
        }
        return new TestSuite("XAJNDITest cannot run without XA support");
    }
    
    public void tearDown() throws Exception {
        ldapServer=null;
        ldapPort=null;
        // need to hold on to dnString value and ic as the fixture runs
        // twice (embedded & networkserver) and they're used inside it
        super.tearDown();
    }

    private InitialDirContext getInitialDirContext()
    {
        try {
            Hashtable env = new Hashtable();
            // using properties - these will have been passed in.
            String ldapContextFactory=getSystemProperty("derbyTesting.ldapContextFactory");
            if (ldapContextFactory == null || ldapContextFactory.length() < 1)
                env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            else
                env.put(Context.INITIAL_CONTEXT_FACTORY, ldapContextFactory);
            env.put(Context.PROVIDER_URL, "ldap://" + ldapServer + ":" + ldapPort);
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            return new InitialDirContext(env);
        } catch (NamingException ne) {
            fail("naming exception ");
            return null;
        }
    }
    
    public void testCompareXADataSourcewithJNDIDataSource()
    throws Exception
    {
            XADataSource xads = J2EEDataSource.getXADataSource();
            String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
            JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
            JDBCDataSource.setBeanProperty(xads, "createDatabase", "create");
            JDBCDataSource.setBeanProperty(xads, "description", "XA DataSource");
            
            ic.rebind("cn=compareDS, o=" + dnString, xads);
            javax.sql.XADataSource ads =
                (javax.sql.XADataSource)ic.lookup("cn=compareDS, o=" + dnString);

            // Embedded data sources implement equals(), so use it to check
            // that the two data sources are equal.
            if (usingEmbedded())
            {
                assertEquals(xads, ads);
            }

            // Client data sources don't implement equals(), so compare each
            // property manually. And by the way, we don't trust that equals()
            // in embedded data sources checks all the properties, so do a
            // full check for embedded as well.
            String[] orgprops = getPropertyBeanList(xads);
            String[] bindprops = getPropertyBeanList(ads);
            assertEquals(orgprops.length, bindprops.length);

            // Check that all properties are equal.
            for (int i=0;i<orgprops.length;i++){
                assertEquals(orgprops[i], bindprops[i]);
                assertEquals(
                        JDBCDataSource.getBeanProperty(xads, orgprops[i]),
                        JDBCDataSource.getBeanProperty(ads, bindprops[i]));
            }

            // modify something essential of the original XADataSource
            JDBCDataSource.clearStringBeanProperty(xads, "createDatabase");
            
            // Now the ads is no longer the same
            assertFalse(xads.equals(ads));
    }

    /**
     * Obtains a list of bean properties through reflection.
     * 
     *
     * @param ds the data source to investigate
     * @return A list of bean property names.
     */
    private static String[] getPropertyBeanList(Object ds) {
        Method[] allMethods = ds.getClass().getMethods();
        ArrayList properties = new ArrayList();

        for (int i = 0; i < allMethods.length; i++) {
            Method method = allMethods[i];
            String methodName = method.getName();
            // Need at least getXX
            if (methodName.length() < 5 || !methodName.startsWith("get") ||
                    method.getParameterTypes().length != 0) {
                continue;
            }

            Class rt = method.getReturnType();
            if (rt.equals(Integer.TYPE) || rt.equals(String.class) ||
                    rt.equals(Boolean.TYPE) || rt.equals(Short.TYPE) ||
                    rt.equals(Long.TYPE)) {
                // Valid Java Bean property.
                // Convert name:
                //    getPassword -> password
                //    getRetrieveMessageText -> retrieveMessageText
                String beanName = methodName.substring(3,4).toLowerCase()
                        + methodName.substring(4);

                properties.add(beanName);
            } else {
                assertFalse("Method '" + methodName + "' with primitive " +
                    "return type not supported - update test!!",
                    rt.isPrimitive());
            }
        }
        return (String[])properties.toArray(new String[properties.size()]);
    }
}