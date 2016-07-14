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

package com.splicemachine.dbTesting.functionTests.tests.derbynet;

import com.splicemachine.dbTesting.functionTests.tests.tools.dblook_test;

public class dblook_test_net_territory extends dblook_test {

	// This test runs dblook on a test database using
	// a connection to the Network Server.

	public static void main (String [] args) {

		System.out.println("\n-= Start dblook (net server) Test. =-");

		territoryBased = ";territory=nl_NL;collation=TERRITORY_BASED";
		testDirectory = "territory_dblook_test_net/";
		expectedCollation = "TERRITORY_BASED";
		separator = System.getProperty("file.separator");
		new dblook_test_net_territory().doTest();
		System.out.println("\n[ Done. ]\n");
		renameDbLookLog("dblook_test_net_territory");

	}

	/* **********************************************
	 * doTest
	 * Run a test of the dblook utility using
	 * Network Server.
	 ****/

	protected void doTest() {

		try {

			createTestDatabase(dbCreationScript_1);

			// Don't let error stream ruin the diff.
			System.err.close();

			// The only test we need to run is the one for
			// Network Server; see functionTests/tools/
			// dblook_test.java.
			runTest(3, testDBName, testDBName + "_new");

		} catch (Exception e) {
			System.out.println("-=- FAILED: to complete the test:");
			e.printStackTrace();
		}

	}

}
