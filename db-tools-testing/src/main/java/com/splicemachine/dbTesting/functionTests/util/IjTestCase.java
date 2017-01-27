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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;

//import com.splicemachine.db.tools.ij;
import com.splicemachine.dbTesting.junit.SupportFilesSetup;


/**
 * Run a .sql script via ij's main method and compare with a canon.
 * 
 * Tests that extend this class should always wrap their suite with
 * a SupportFilesSetup so that the extinout directory where ij will
 * write the test output is created. 
 */
public class IjTestCase extends ScriptTestCase {

	String scriptName;
	String outfileName;
    File outfile;
	
    /**
     * Create a script testcase that runs the .sql script with the
     * given name. The name should not include the ".sql" suffix.
     */
	public IjTestCase(String name) {
		super(name);
		scriptName = getName() + ".sql";
		outfileName = SupportFilesSetup.EXTINOUT + "/" + getName() + ".out";
		outfile = new File(outfileName);
	}
	
	public void setUp() {
	    super.setUp();
		setSystemProperty("ij.outfile", outfileName);
		setSystemProperty("ij.defaultResourcePackage",
				"/com/splicemachine/dbTesting/functionTests/tests/"
				+ getArea() + "/");
	}
	
	public void tearDown() throws Exception {
		super.tearDown();
		removeSystemProperty("ij.outfile");
		removeSystemProperty("ij.defaultResourcePackage");
	}
	
	/**
	 * Run a .sql test, calling ij's main method.
	 * Then, take the output file and read it into our OutputStream
	 * so that it can be compared via compareCanon().
	 * TODO:
	 * Note that the output will include a version number;
	 * this should get filtered/ignored in compareCanon
	 */
	public void runTest() throws Throwable {
		String [] args = { "-fr", scriptName };
		// splice ij.main(args);
		
		String canon =
			"com/splicemachine/dbTesting/functionTests/master/"
			+ getName() + ".out";
		
		final File out = outfile;
		FileInputStream fis = (FileInputStream) AccessController.doPrivileged(new PrivilegedAction() {
			public Object run() {
				FileInputStream fis = null;
				try {
					fis = new FileInputStream(out);
				} catch (FileNotFoundException e) {
					fail("Could not open ij output file.");
				}				
				return fis;
			}
		});
		OutputStream os = getOutputStream();
		int b;
		while ((b = fis.read()) != -1) {
			os.write(b);
		}
		fis.close();
		
		Boolean deleted = (Boolean) AccessController.doPrivileged(new PrivilegedAction() {
			public Object run() {
				boolean d = outfile.delete();
				
				return new Boolean(d);
			}
		});
		
		if (!deleted.booleanValue())
			println("Could not delete outfile for " + scriptName);
		
		this.compareCanon(canon);
	}
}
