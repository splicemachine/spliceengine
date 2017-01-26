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
package com.splicemachine.dbTesting.functionTests.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedActionException;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;

/**
 * Run a test that compares itself to a master (canon) file.
 * This is used to support cannon based tests that ran
 * under the old Derby test harness without having to convert
 * them. It is not recommended for new tests. New test should
 * use the JUnit assert mechanisms.
 *
 */
abstract class CanonTestCase extends BaseJDBCTestCase {

    final static String DEFAULT_ENCODING = "US-ASCII";
    final String outputEncoding;

    private ByteArrayOutputStream rawBytes;

    CanonTestCase(String name) {
        this(name, null);
    }

    CanonTestCase(String name, String encoding) {
        super(name);
        outputEncoding = (encoding == null) ? DEFAULT_ENCODING : encoding;
    }

    OutputStream getOutputStream() {
        return rawBytes = new ByteArrayOutputStream(20 * 1024);
    }

    /**
     * Compare the output to the canon provided.
     * 
     * @param canon
     *            Name of canon as a resource.
     */
    void compareCanon(String canon) throws Throwable {
        rawBytes.flush();
        rawBytes.close();

        byte[] testRawBytes = rawBytes.toByteArray();
        rawBytes = null;
        BufferedReader cannonReader = null;
        BufferedReader testOutput = null;

        try {
            URL canonURL = getTestResource(canon);
            assertNotNull("No master file " + canon, canonURL);

            InputStream canonStream = openTestResource(canonURL);

            cannonReader = new BufferedReader(
                    new InputStreamReader(canonStream, outputEncoding));

            testOutput = new BufferedReader(
                    new InputStreamReader(
                            new ByteArrayInputStream(testRawBytes),
                            outputEncoding));

            for (int lineNumber = 1;; lineNumber++) {
                String testLine = testOutput.readLine();

                String canonLine = cannonReader.readLine();

                if (canonLine == null && testLine == null)
                    break;

                if (canonLine == null)
                    fail("More output from test than expected");

                if (testLine == null)
                    fail("Less output from test than expected, stoped at line"
                            + lineNumber);

                assertEquals("Output at line " + lineNumber, canonLine,
                        testLine);
            }
        } catch (Throwable t) {
            dumpForFail(testRawBytes);
            throw t;
        } finally {
            if (cannonReader != null) {
                try {
                    cannonReader.close();
                } catch (IOException e) {
                }
            }
            
            if (testOutput != null) {
                try {
                    testOutput.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * Dump the output that did not compare correctly into the failure folder
     * with the name this.getName() + ".out".
     * 
     * @param rawOutput
     * @throws IOException
     * @throws PrivilegedActionException
     */
    private void dumpForFail(byte[] rawOutput) throws IOException,
            PrivilegedActionException {

        File folder = getFailureFolder();
        final File outFile = new File(folder, getName() + ".out");

        OutputStream outStream = (OutputStream) AccessController
                .doPrivileged(new java.security.PrivilegedExceptionAction() {

                    public Object run() throws IOException {
                        return new FileOutputStream(outFile);
                    }
                });

        outStream.write(rawOutput);
        outStream.flush();
        outStream.close();
    }
}
