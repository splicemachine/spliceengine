/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
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
