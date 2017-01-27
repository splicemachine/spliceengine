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

package com.splicemachine.dbTesting.functionTests.tests.tools;

import java.io.PrintStream;
import java.security.AccessController;
import java.security.PrivilegedAction;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.db.tools.ij;
import com.splicemachine.dbTesting.functionTests.util.TestNullOutputStream;
import com.splicemachine.dbTesting.junit.BaseTestCase;
import com.splicemachine.dbTesting.junit.SupportFilesSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

public class IjSecurityManagerTest extends BaseTestCase {

	public IjSecurityManagerTest(String name) {
		super(name);
	}

	public void testRunIJ() throws Exception {
	    /* Save the original out stream */
	    final PrintStream out = System.out;
	    
	    /* Mute the test */
	    AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                System.setOut(new PrintStream(new TestNullOutputStream()));
                return null;
            }
        });
	    
	    try {
	        /* Run ij */
	        ij.main(new String[]{"extinout/IjSecurityManagerTest.sql"});
	    } catch (Exception e) { /* Should NEVER happen */
	        fail("Failed to run ij under security manager.",e);
	    } finally {
	        /* Restore the original out stream */
	        AccessController.doPrivileged(new PrivilegedAction() {
	            public Object run() {
	                System.setOut(out);
	                return null;
	            }
	        });
	    }
	}
	
	private static Test decorateTest() {	    
	    Test test = TestConfiguration.embeddedSuite(IjSecurityManagerTest.class);
        test = new SupportFilesSetup
         (
          test,
          null,
          new String[] { "functionTests/tests/tools/IjSecurityManagerTest.sql"  },
          null,
          new String[] { "IjSecurityManagerTest.sql"}
          );
        return test;
	}
	public static Test suite() {		
		TestSuite suite = new TestSuite("IjSecurityManagerTest");
		suite.addTest(decorateTest());
		return suite;
	}
}
