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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;
import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.Derby;
import com.splicemachine.dbTesting.junit.SupportFilesSetup;

/**
 * Makes sure that old serialized data sources can be de-serialized with the
 * current version of the data souce.
 * <p>
 * Serialized data source from old versions are expected to be found in
 * <tt>testData/serializedDataSources</tt>, with the following filename
 * format CLASSNAME-VERSION.ser, where CLASSNAME is the unqualified name of the
 * data source class, and VERSION is the Derby version. An example:
 * <tt>ClientPooledConnectionDataSource-10_1.ser</tt>
 * <p>
 * A separation between JDBC 4.0 specific classes and the other classes is not
 * made.
 * <p>
 * This test should detect the typical incompatible changes in the current
 * data source implementations, for instance deleting a field or changing its
 * type.
 */
public class DataSourceSerializationTest
        extends BaseJDBCTestCase {

    /** Constant for Derby version 10.0.2.1. */
    private static final String VERSION_10_0_2_1 = "10_0_2_1";
    /** Constant for Derby version 10.1.3.1. */
    private static final String VERSION_10_1_3_1 = "10_1_3_1";
    /** Constant for Derby version 10.2.2.0 */
    private static final String VERSION_10_2_2_0 = "10_2_2_0";
    /** Constant for Derby version 10.3.2.1. */
    private static final String VERSION_10_3_2_1 = "10_3_2_1";

    public DataSourceSerializationTest(String name) {
        super(name);
    }

    /**
     * Tests the de-serialization of the basic embedded data source.
     *
     * @throws Exception for a number of error conditions
     */
    public void serTestEmbeddedDataSource()
            throws Exception {
        final String EMBEDDED_CLASS = "EmbeddedDataSource";
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_0_2_1);
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_1_3_1);
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_2_2_0);
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_3_2_1);
    }

    /**
     * Tests the de-serialization of the embedded connection pool data source.
     *
     * @throws Exception for a number of error conditions
     */
    public void serTestEmbeddedConnectionPoolDataSource()
            throws Exception {
        final String EMBEDDED_CLASS = "EmbeddedConnectionPoolDataSource";
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_0_2_1);
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_1_3_1);
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_2_2_0);
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_3_2_1);
    }

    /**
     * Tests the de-serialization of the embedded XA data source.
     *
     * @throws Exception for a number of error conditions
     */
    public void serTestEmbeddedXADataSource()
            throws Exception {
        final String EMBEDDED_CLASS = "EmbeddedXADataSource";
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_0_2_1);
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_1_3_1);
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_2_2_0);
        deSerializeDs(EMBEDDED_CLASS, VERSION_10_3_2_1);
    }

    /**
     * Tests the de-serialization of the basic client data source.
     *
     * @throws Exception for a number of error conditions
     */
    public void serTestClientDataSource()
            throws Exception {
        final String CLIENT_CLASS = "ClientDataSource";
        // No client driver for Derby 10.0
        deSerializeDs(CLIENT_CLASS, VERSION_10_1_3_1);
        deSerializeDs(CLIENT_CLASS, VERSION_10_2_2_0);
        deSerializeDs(CLIENT_CLASS, VERSION_10_3_2_1);
    }

    /**
     * Tests the de-serialization of the client connection pool data source.
     *
     * @throws Exception for a number of error conditions
     */
    public void serTestClientConnectionPoolDataSource()
            throws Exception {
        final String CLIENT_CLASS = "ClientConnectionPoolDataSource";
        // No client driver for Derby 10.0
        deSerializeDs(CLIENT_CLASS, VERSION_10_1_3_1);
        deSerializeDs(CLIENT_CLASS, VERSION_10_2_2_0);
        deSerializeDs(CLIENT_CLASS, VERSION_10_3_2_1);
    }

    /**
     * Tests the de-serialization of the client XA data source.
     *
     * @throws Exception for a number of error conditions
     */
    public void serTestClientXADataSource()
            throws Exception {
        final String CLIENT_CLASS = "ClientXADataSource";
        // No client driver for Derby 10.0
        deSerializeDs(CLIENT_CLASS, VERSION_10_1_3_1);
        deSerializeDs(CLIENT_CLASS, VERSION_10_2_2_0);
        deSerializeDs(CLIENT_CLASS, VERSION_10_3_2_1);
    }

    /**
     * Attempts to de-serialize a data source object from a file.
     * <p>
     * <ol> <li>Derby version string - UTF</li>
     *      <li>Derby build number - UTF</li>
     *      <li>Derby data source - object</li>
     *      <li>Derby data source reference - object</li>
     * </ol>
     * <p>
     * If the object is successfully instantiated and cast to
     * {@link javax.sql.DataSource}
     *
     * @param className name of the class to de-serialize
     * @param version Derby version
     *
     * @throws Exception on a number of error conditions
     */
    private void deSerializeDs(String className, String version)
            throws Exception {
        // Construct the filename
        final StringBuffer fname = new StringBuffer(className);
        fname.append('-');
        fname.append(version);
        fname.append(".ser");

        // De-serialize the data source.
        InputStream is;
        try {
            is = (FileInputStream)AccessController.doPrivileged(
                  new PrivilegedExceptionAction() {
                public Object run() throws FileNotFoundException {
                    return new FileInputStream(
                            SupportFilesSetup.getReadOnly(fname.toString()));
                }
            });
            } catch (PrivilegedActionException e) {
                // e.getException() should be a FileNotFoundException.
                throw (FileNotFoundException)e.getException();
            }

        assertNotNull("FileInputStream is null", is);
        Object dsObj = null;
        DataSource ds = null;
        Reference dsRef = null;
        // Used to preserve original error information in case of exception when 
        // closing the input stream.
        boolean testSequencePassed = false;
        try {
            ObjectInputStream ois = new ObjectInputStream(is);
            String buildVersion = ois.readUTF();
            String buildNumber = ois.readUTF();
            println("Data source " + className + ", version " +
                    buildVersion + ", build " + buildNumber);
            dsObj = ois.readObject();
            assertNotNull("De-serialized data source is null", dsObj);
            assertTrue("Unexpected class instantiated: " +
                    dsObj.getClass().getName(),
                    dsObj.getClass().getName().indexOf(className) > 0);
            ds = (DataSource)dsObj;
            // Just see if the object is usable.
            int newTimeout = ds.getLoginTimeout() +9;
            assertFalse(ds.getLoginTimeout() == newTimeout);
            ds.setLoginTimeout(newTimeout);
            assertEquals(newTimeout, ds.getLoginTimeout());

            // Recreate the data source using reference.
            dsRef = (Reference)ois.readObject();
            ois.close();
            testSequencePassed = true;
        } finally {
            if (testSequencePassed) {
                is.close();
            } else {
                try {
                    is.close();
                } catch (IOException ioe) {
                    // Ignore this to preserve the original exception.
                }
            }
        }

        String factoryClassName = dsRef.getFactoryClassName();
        ObjectFactory factory =
            (ObjectFactory)Class.forName(factoryClassName).newInstance();
        Object recreatedDs =
            factory.getObjectInstance(dsRef, null, null, null);
        ds = (DataSource)recreatedDs;
        assertTrue("Unexpected class instantiated by Reference: " +
                dsObj.getClass().getName(),
                dsObj.getClass().getName().indexOf(className) > 0);
    }

    /**
     * Returns an appropariate suite of tests to run.
     *
     * @return A test suite.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("DataSourceSerializationTest");
        String filePrefix = "functionTests/testData/serializedDataSources/";
        // De-serialize embedded data sources only if we have the engine code.
        if (Derby.hasEmbedded()) {
            suite.addTest(new DataSourceSerializationTest(
                    "serTestEmbeddedDataSource"));
            suite.addTest(new DataSourceSerializationTest(
                    "serTestEmbeddedConnectionPoolDataSource"));
            suite.addTest(new DataSourceSerializationTest(
                    "serTestEmbeddedXADataSource"));
        }

        // De-serialize client data sources only if we have the client code.
        if (Derby.hasClient()) {
            suite.addTest(new DataSourceSerializationTest(
                    "serTestClientDataSource"));
            suite.addTest(new DataSourceSerializationTest(
                    "serTestClientConnectionPoolDataSource"));
            suite.addTest(new DataSourceSerializationTest(
                    "serTestClientXADataSource"));
        }

        return new SupportFilesSetup(suite, new String[] {
                // 10.0 resources
                filePrefix + "EmbeddedDataSource-10_0_2_1.ser",
                filePrefix + "EmbeddedConnectionPoolDataSource-10_0_2_1.ser",
                filePrefix + "EmbeddedXADataSource-10_0_2_1.ser",

                // 10.1 resources
                filePrefix + "EmbeddedDataSource-10_1_3_1.ser",
                filePrefix + "EmbeddedConnectionPoolDataSource-10_1_3_1.ser",
                filePrefix + "EmbeddedXADataSource-10_1_3_1.ser",
                filePrefix + "ClientDataSource-10_1_3_1.ser",
                filePrefix + "ClientConnectionPoolDataSource-10_1_3_1.ser",
                filePrefix + "ClientXADataSource-10_1_3_1.ser",

                // 10.2 resources
                filePrefix + "EmbeddedDataSource-10_2_2_0.ser",
                filePrefix + "EmbeddedConnectionPoolDataSource-10_2_2_0.ser",
                filePrefix + "EmbeddedXADataSource-10_2_2_0.ser",
                filePrefix + "ClientDataSource-10_2_2_0.ser",
                filePrefix + "ClientConnectionPoolDataSource-10_2_2_0.ser",
                filePrefix + "ClientXADataSource-10_2_2_0.ser",

                // 10.3 resources
                filePrefix + "EmbeddedDataSource-10_3_2_1.ser",
                filePrefix + "EmbeddedConnectionPoolDataSource-10_3_2_1.ser",
                filePrefix + "EmbeddedXADataSource-10_3_2_1.ser",
                filePrefix + "ClientDataSource-10_3_2_1.ser",
                filePrefix + "ClientConnectionPoolDataSource-10_3_2_1.ser",
                filePrefix + "ClientXADataSource-10_3_2_1.ser",
            });
    }
}

