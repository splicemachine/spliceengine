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

package com.splicemachine.dbTesting.functionTests.tests.engine;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import junit.framework.Test;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;


/**
 * Tests related to the Derby error stream.
 *
 * This test has been converted to junit from the old harness tests
 * logStream.java and errorStream.java. The testDefault method is based on
 * logStream.java, the other test* methods are from errorStream.java.
 */
public class ErrorStreamTest extends BaseJDBCTestCase {

    private static final String FILE_PROP   = "derby.stream.error.file";
    private static final String METHOD_PROP = "derby.stream.error.method";
    private static final String FIELD_PROP  = "derby.stream.error.field";

    /**
     * runNo keeps track of which run we are in to generate unique (within a
     * JUnit run) names for files that are used in the test. Has to be static.
     */
    private static int runNo = 0;

    /**
     * File used when FILE_PROP is set, it maps to file
     * <database>-file-<runNo>.log
     */
    private File fileStreamFile;

    /**
     * See doc for getStream() below. Has to be static.
     */
    private static OutputStream methodStream;
    private File methodStreamFile;

    /**
     * Field fieldStream used by Derby when FIELD_PROP is set,
     * so it needs to be public and static.
     * Maps to file <database>-field-<runNo>.log
     */
    public static OutputStream fieldStream;
    private File fieldStreamFile;

    /**
     * Field errStream used as redirection for System.err to be able
     * to checks its (non-)use in the scenarios. We first tried to
     * merge it with System.out and let the harness compare outputs,
     * but this gave intermittent merging differences, so abandoned.
     * Maps to file <database>-err-<runNo>.log
     */
    private OutputStream errStream;
    private File errStreamFile;

    public ErrorStreamTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.embeddedSuite(ErrorStreamTest.class);
    }

    public void setUp() throws Exception {
        bootDerby();
        // Shutdown engine so we can change properties for error stream
        getTestConfiguration().shutdownEngine();
        openStreams();
    }

    public void tearDown() throws Exception {
        resetProps();
        deleteStreamFiles();
        nullFields();
        super.tearDown();
    }

    /**
     * Test that the error stream file (db.log) is created at database boot
     * and not deleted when the database is shut down, but can be deleted
     * afterwards.
     */
    public void testDefault() throws IOException, SQLException {
        File derbyLog = new File(getSystemProperty("derby.system.home"),
              "derby.log");
        bootDerby();
        assertIsExisting(derbyLog);
        assertNotDirectory(derbyLog);
        assertNotEmpty(derbyLog);

        println("Shutdown database");
        getTestConfiguration().shutdownDatabase();

        assertIsExisting(derbyLog);
        assertNotDirectory(derbyLog);
        assertNotEmpty(derbyLog);

        println("Shutdown engine");
        getTestConfiguration().shutdownEngine();

        boolean deleted = deleteFile(derbyLog);
        assertTrue("File " + derbyLog + " could not be deleted", deleted);
    }

    /**
     * Test the db.stream.error.file property.
     */
    public void testFile() throws IOException, SQLException {
        setSystemProperty(FILE_PROP, getCanonicalPath(fileStreamFile));

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotEmpty(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test the db.stream.error.file property with wrong input.
     */
    public void testWrongFile() throws IOException, SQLException {
        setSystemProperty(FILE_PROP, getCanonicalPath(new File(
              new File(getSystemProperty("derby.system.home"), "foo"),
              makeStreamFilename("file")))); // erroneous path

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertNotEmpty(errStreamFile);
    }

    /**
     * Test the db.stream.error.method property.
     */
    public void testMethod() throws IOException, SQLException  {
        setSystemProperty(METHOD_PROP,
              "com.splicemachine.dbTesting.functionTests.tests.engine."+
              "ErrorStreamTest.getStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(fileStreamFile);
        assertNotEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test the db.stream.error.method property with wrong input.
     */
    public void testWrongMethod() throws IOException, SQLException {
        setSystemProperty(METHOD_PROP,
              "com.splicemachine.dbTesting.functionTests.tests.engine."+
              "ErrorStreamTest.nonExistingGetStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertNotEmpty(errStreamFile);
    }

    /**
     * Test the db.stream.error.field property.
     */
    public void testField() throws IOException, SQLException {
        setSystemProperty(FIELD_PROP,
              "com.splicemachine.dbTesting.functionTests.tests.engine."+
              "ErrorStreamTest.fieldStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertNotEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test the db.stream.error.field property with wrong input.
     */
    public void testWrongField() throws IOException, SQLException {
        setSystemProperty(FIELD_PROP,
              "com.splicemachine.dbTesting.functionTests.tests.engine."+
              "ErrorStreamTest.nonExistingFieldStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertNotEmpty(errStreamFile);
    }

    /**
     * Test that the db.stream.error.file property overrides the
     * db.stream.error.method property.
     */
    public void testFileOverMethod() throws IOException, SQLException {
        setSystemProperty(FILE_PROP, getCanonicalPath(fileStreamFile));
        setSystemProperty(METHOD_PROP,
              "com.splicemachine.dbTesting.functionTests.tests.engine."+
              "ErrorStreamTest.getStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotEmpty(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test that the db.stream.error.file property overrides the
     * db.stream.error.field property.
     */
    public void testFileOverField() throws IOException, SQLException {
        setSystemProperty(FILE_PROP, getCanonicalPath(fileStreamFile));
        setSystemProperty(FIELD_PROP,
              "com.splicemachine.dbTesting.functionTests.tests.engine."+
              "ErrorStreamTest.fieldStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotEmpty(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test that the db.stream.error.file property overrides the
     * db.stream.error.method and the db.stream.error.field property.
     */
    public void testFileOverMethodAndField() throws IOException, SQLException {
        setSystemProperty(FILE_PROP, getCanonicalPath(fileStreamFile));
        setSystemProperty(METHOD_PROP,
              "com.splicemachine.dbTesting.functionTests.tests.engine."+
              "ErrorStreamTest.getStream");
        setSystemProperty(FIELD_PROP,
              "com.splicemachine.dbTesting.functionTests.tests.engine."+
              "ErrorStreamTest.fieldStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotEmpty(fileStreamFile);
        assertIsEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Test that the db.stream.error.field property overrides the
     * db.stream.error.method property.
     */
    public void testMethodOverField() throws IOException, SQLException {

        setSystemProperty(METHOD_PROP,
              "com.splicemachine.dbTesting.functionTests.tests.engine."+
              "ErrorStreamTest.getStream");
        setSystemProperty(FIELD_PROP,
              "com.splicemachine.dbTesting.functionTests.tests.engine."+
              "ErrorStreamTest.fieldStream");

        bootDerby();
        getTestConfiguration().shutdownEngine();

        closeStreams();

        assertNotExisting(fileStreamFile);
        assertNotEmpty(methodStreamFile);
        assertIsEmpty(fieldStreamFile);
        assertIsEmpty(errStreamFile);
    }

    /**
     * Method getStream used by Derby when db.stream.error.method
     * is set.  Maps to file <database>-method-<runNo>.log
     * This method has to be static.
     */
    public static OutputStream getStream() {
        return methodStream;
    }

    private static String makeStreamFilename(String type) {
        return type + "-" + runNo + ".log";
    }

    private void openStreams() throws IOException{
        String systemHome = getSystemProperty("derby.system.home");
        makeDirIfNotExisting(systemHome);

        runNo += 1;

        methodStreamFile = new File(systemHome, makeStreamFilename("method"));
        fileStreamFile = new File(systemHome, makeStreamFilename("file"));
        fieldStreamFile = new File(systemHome, makeStreamFilename("field"));
        errStreamFile = new File(systemHome, makeStreamFilename("err"));

        methodStream = newFileOutputStream(methodStreamFile);
        fieldStream = newFileOutputStream(fieldStreamFile);
        errStream = newFileOutputStream(errStreamFile);

        setSystemErr(new PrintStream(errStream));

    }


    private void closeStreams() throws IOException {
        try {
            methodStream.close();
            fieldStream.close();
            errStream.close();

            // reset until next scenario, no expected output
            setSystemErr(System.out);
        } catch (IOException e) {
            println("Could not close stream files");
            throw e;
        }
    }


    private static void assertIsDirectory(final File f) throws IOException {
        try {
            AccessController.doPrivileged (new PrivilegedExceptionAction() {
                public Object run() throws IOException {
                    assertTrue("assertIsDirectory failed: " +
                          f.getCanonicalPath(), f.isDirectory());
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of IOException.
            throw (IOException) e.getException();
        }
    }

    private static void assertNotDirectory(final File f) throws IOException {
        try {
            AccessController.doPrivileged (new PrivilegedExceptionAction() {
                public Object run() throws IOException {
                    assertFalse("assertNotDirectory failed: " +
                          f.getCanonicalPath(), f.isDirectory());
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of IOException.
            throw (IOException) e.getException();
        }
    }

    private static void assertIsEmpty(final File f) throws IOException {
        try {
            AccessController.doPrivileged (new PrivilegedExceptionAction() {
                public Object run() throws IOException {
                    assertTrue("assertIsEmpty failed: " + f.getCanonicalPath(),
                          (f.exists() && (f.length() == 0)));
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of IOException.
            throw (IOException) e.getException();
        }
    }


    private static void assertNotEmpty(final File f) throws IOException {
        try {
            AccessController.doPrivileged (new PrivilegedExceptionAction() {
                public Object run() throws IOException {
                    assertTrue("assertNotEmpty failed: " + f.getCanonicalPath()
                          + " does not exist.", f.exists());
                    FileInputStream fis = new FileInputStream(f);
                    int result = fis.read();
                    fis.close();
                    assertTrue("assertNotEmpty failed: " + f.getCanonicalPath()
                          + " is empty.", -1 != result);
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of IOException.
            throw (IOException) e.getException();
        }
    }

    private static void assertIsExisting(final File f) throws IOException {
        try {
            AccessController.doPrivileged (new PrivilegedExceptionAction() {
                public Object run() throws IOException {
                    assertTrue("assertIsExisting failed: " +
                          f.getCanonicalPath(), f.exists());
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of IOException.
            throw (IOException) e.getException();
        }
    }


    private static void assertNotExisting(final File f) throws IOException {
        try {
            AccessController.doPrivileged (new PrivilegedExceptionAction() {
                public Object run() throws IOException {
                    assertFalse("assertNotExisting failed: " +
                          f.getCanonicalPath(), f.exists());
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of IOException.
            throw (IOException) e.getException();
        }
    }

    private static boolean deleteFile(final File f) {
        Boolean deleted = (Boolean) AccessController.doPrivileged(
              new PrivilegedAction() {
            public Object run() {
                return new Boolean(f.delete());
            }
        });
        return deleted.booleanValue();
    }

    private static String getCanonicalPath(final File f) throws IOException {
        String path = null;
        try {
            path = (String) AccessController.doPrivileged(
                  new PrivilegedExceptionAction() {
                public Object run() throws IOException {
                    return f.getCanonicalPath();
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of IOException.
            throw (IOException) e.getException();
        }
        return path;
    }

    private static void makeDirIfNotExisting(final String filename) {
        AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                File f = new File(filename);
                if(!f.exists()) {
                    f.mkdir();
                }
                return null;
            }
        });
    }

    private static void setSystemErr(final PrintStream err) {
        AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                System.setErr(err);
                return null;
            }
        });
    }

    private static FileOutputStream newFileOutputStream(final File f)
    throws FileNotFoundException {
        FileOutputStream outStream = null;
        try {
            outStream = (FileOutputStream) AccessController.doPrivileged(
                  new PrivilegedExceptionAction() {
                public Object run() throws FileNotFoundException {
                    return new FileOutputStream(f);
                }
            });
            } catch (PrivilegedActionException e) {
                // e.getException() should be a FileNotFoundException.
                throw (FileNotFoundException) e.getException();
            }
        return outStream;
    }


    private static void resetProps() {
        removeSystemProperty(FILE_PROP);
        removeSystemProperty(METHOD_PROP);
        removeSystemProperty(FIELD_PROP);
    }

    private void deleteStreamFiles() {
        deleteFile(fileStreamFile);
        deleteFile(methodStreamFile);
        deleteFile(fieldStreamFile);
        deleteFile(errStreamFile);
    }

    private void nullFields() {
        // Nulling fields to let objects be gc'd
        fileStreamFile = null;
        methodStreamFile = null;
        fieldStreamFile = null;
        errStreamFile = null;
        methodStream = null;
        fieldStream = null;
        errStream = null;
    }


    private void bootDerby() throws SQLException {
        /* Connect to the database to make sure that the
         * JDBC Driver class is loaded
         */
        getConnection();
        getConnection().close();
    }

}
