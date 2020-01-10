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
package com.splicemachine.dbTesting.functionTests.util;

import java.io.InputStream;
import java.net.URL;
import java.security.AccessController;
import java.sql.Connection;
import java.util.Locale;

import com.splicemachine.dbTesting.junit.Derby;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Run a .sql script as a test comparing it to
 * a master output file.
 *
 */
public abstract class ScriptTestCase extends CanonTestCase {

	private final String inputEncoding;
	private final String user;
    private boolean useSystemProperties = false;
    private Locale oldLocale;

    /**
	 * Create a ScriptTestCase to run a single test
     * using a connection obtained from getConnection()
	 * @param script Base name of the .sql script
     * @param useSystemProperties Whether to use system properties for this test
	 * excluding the .sql suffix.
	 */
	public ScriptTestCase(String script, boolean useSystemProperties)
	{
        this(script, null, null, null);
        this.useSystemProperties = useSystemProperties;
	}

	/**
	 * Create a ScriptTestCase to run a single test
     * using a connection obtained from getConnection()
	 * @param script Base name of the .sql script
	 * excluding the .sql suffix.
	 */
	public ScriptTestCase(String script)
	{
        this(script, null, null, null);
	}
	
    /**
     * Create a ScriptTestCase to run a single test
     * using a connection obtained from getConnection() with a
     * different encoding
     * @param script Base name of the .sql script
     * excluding the .sql suffix.
     */
    public ScriptTestCase(String script, String encoding)
    {
        this(script, encoding, encoding, null);
    }

    /**
     * Create a ScriptTestCase to run a single test
     * using a connection obtained from getConnection() with a
     * different encoding.
     * @param script     Base name of the .sql script
     *                   excluding the .sql suffix.
     * @param inputEnc   The encoding for the script, if not null,
     *                   else use "US-ASCII"
     * @param outputEnc  The encoding for the ouput from the script,
     *                   if not null, else use "US-ASCII"
     * @param user       Run script as user
     */
    public ScriptTestCase(String script,
            String inputEnc, String outputEnc, String user)
    {
        super(script, outputEnc);
        setSystemProperty("ij.showNoConnectionsAtStart", "true");
        setSystemProperty("ij.showNoCountForSelect", "true");
        inputEncoding = (inputEnc == null) ? DEFAULT_ENCODING : inputEnc;

		this.user = user;
    }

    /**
	 * Return the folder (last element of the package) where
	 * the .sql script lives, e.g. lang.
	 */
	protected String getArea() {
		
		String name =  getClass().getName();
		
		int lastDot = name.lastIndexOf('.');
		
		name = name.substring(0, lastDot);
		
		lastDot = name.lastIndexOf('.');
		
		return name.substring(lastDot+1);
	}
		
	/**
	 * Get a decorator to setup the ij in order
	 * to run the test. A sub-class must decorate
	 * its suite using this call.
	 */
	public static Test getIJConfig(Test test)
	{
        // Need the tools to run the scripts as this
        // test uses ij as the script runner.
        if (!Derby.hasTools())
            return new TestSuite("empty: no tools support");
            
		// No decorator needed currently.
		return test;
	}
	
	/**
	 * Run the test, using the resource as the input.
	 * Compare to the master file using a very simple
	 * line by line comparision. Fails at the first
	 * difference. If a failure occurs the output
	 * is written into the current directory as
	 * testScript.out, otherwise the output is only
	 * kept in memory.
	 * @throws Throwable 
	 */
	public void runTest() throws Throwable
	{
		String resource =
			"com/splicemachine/dbTesting/functionTests/tests/"
			+ getArea() + "/"
			+ getName() + ".sql";
		
		String canon =
			"com/splicemachine/dbTesting/functionTests/master/"
			+ getName() + ".out";

		URL sql = getTestResource(resource);
		assertNotNull("SQL script missing: " + resource, sql);
		
		InputStream sqlIn = openTestResource(sql);

		Connection conn;

		if (user != null) {
			conn = openUserConnection(user);
		} else {
			conn = getConnection();
		}

        final String outputEnc;
        final String derby_ui_codeset = getSystemProperty("derby.ui.codeset");

        if (derby_ui_codeset != null) {
            // IJ should format output according to the db.ui.codeset
            // variable. If we pass in an encoding explicitly to runScript(),
            // we won't test that db.ui.codeset is obeyed. Therefore,
            // leave it as null.
            outputEnc = null;
            assertEquals(
                    "Requested output encoding and db.ui.codeset differ",
                    outputEncoding, derby_ui_codeset);
        } else {
            // db.ui.codeset isn't set. Tell runScript() which output
            // encoding to use.
            outputEnc = outputEncoding;
        }
        
//		com.splicemachine.db.tools.ij.runScript(
//				conn,
//				sqlIn,
//				inputEncoding,
//                getOutputStream(),
//				outputEnc,
//                useSystemProperties);
		
		if (!conn.isClosed() && !conn.getAutoCommit())
		    conn.commit();
		
		sqlIn.close();
        
        this.compareCanon(canon);
	}
    
    /**
     * Set up the new locale for the test
     */
    protected void setUp() {
        oldLocale = Locale.getDefault();

        AccessController.doPrivileged(new java.security.PrivilegedAction() {
            public Object run() {
                Locale.setDefault(Locale.US);
                return null;
            }
        });

        // Reset IJ's locale to allow it to pick up the new locale from
        // the environment.
        throw new UnsupportedOperationException("splice");
    }

    /**
     * Revert the locale back to the old one
     */
    protected void tearDown() throws Exception {
        super.tearDown();

        AccessController.doPrivileged(new java.security.PrivilegedAction() {
            public Object run() {
                Locale.setDefault(oldLocale);
                return null;
            }
        });

        // Forget the locale used by this test.
        throw new UnsupportedOperationException("splice");
    }
}
