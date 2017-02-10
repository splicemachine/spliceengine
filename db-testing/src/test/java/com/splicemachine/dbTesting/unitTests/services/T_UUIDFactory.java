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

package com.splicemachine.dbTesting.unitTests.services;

import com.splicemachine.dbTesting.unitTests.harness.T_Generic;
import com.splicemachine.dbTesting.unitTests.harness.T_Fail;

import  com.splicemachine.db.catalog.UUID;

import com.splicemachine.db.iapi.services.monitor.Monitor;

import com.splicemachine.db.iapi.services.stream.HeaderPrintWriter;

import com.splicemachine.db.iapi.services.uuid.UUIDFactory;

/**
	Test to ensure a implementation of the UUID module
	implements the protocol correctly. 
*/

public class T_UUIDFactory extends T_Generic {

	protected UUIDFactory factory;
	boolean resultSoFar;

	public 	T_UUIDFactory() {
		super();
	}

	protected String getModuleToTestProtocolName() {

		return "A.Dummy.Name";
	}

	/**
		Run all the tests, each test that starts with 'S' is a single user
		test, each test that starts with 'M' is a multi-user test.

		@exception T_Fail The test failed in some way.
	*/
	protected void runTests() throws T_Fail {

		factory = Monitor.getMonitor().getUUIDFactory();
		if (factory == null) {
			throw T_Fail.testFailMsg(getModuleToTestProtocolName() + " module not started.");
		}

		if (!testUUID())
			throw T_Fail.testFailMsg("testUUID indicated failure");
	}


	/*
	** Tests
	*/

	protected boolean testUUID() {
		resultSoFar = true;

		UUID uuid1 = factory.createUUID();
		UUID uuid2 = factory.createUUID();

		if (uuid1.equals(uuid2)){
			// Resolve: format this with a message factory
			String message =  
				"UUID factory created matching UUIDS '%0' and '%1'";
			out.printlnWithHeader(message);
			resultSoFar =  false;
		}

		if (!uuid1.equals(uuid1)){
			// Resolve: format this with a message factory
			String message = 
				"UUID '%0' does not equal itself";
			resultSoFar =  false;
		}

		if (uuid1.hashCode() != uuid1.hashCode()){
			// Resolve: format this with a message factory
			String message = 
				"UUID '%0' does not hash to the same thing twice.";
			out.printlnWithHeader(message);
			resultSoFar =  false;
		}

		// Check that we can go from UUID to string and back.
		
		String suuid1 = uuid1.toString();
		UUID uuid3 = factory.recreateUUID(suuid1);
		if (!uuid3.equals(uuid1)){
			// Resolve: format this with a message factory
			String message = 
				"Couldn't recreate UUID: "
				+ uuid3.toString() 
				+ " != "
				+ uuid1.toString();
			out.printlnWithHeader(message);
			resultSoFar =  false;
		}

		// Check that we can transform from string to UUID and back
		// for a few "interesting" UUIDs.

		// This one came from GUIDGEN.EXE.
		testUUIDConversions(out, "7878FCD0-DA09-11d0-BAFE-0060973F0942");

		// Interesting bit patterns.
		testUUIDConversions(out, "80706050-4030-2010-8070-605040302010");
		testUUIDConversions(out, "f0e0d0c0-b0a0-9080-7060-504030201000");
		testUUIDConversions(out, "00000000-0000-0000-0000-000000000000");
		testUUIDConversions(out, "ffffffff-ffff-ffff-ffff-ffffffffffff");

		// A couple self-generated ones for good measure.
		testUUIDConversions(out, factory.createUUID().toString());
 		testUUIDConversions(out, factory.createUUID().toString());

		return resultSoFar;
	
	}

	private void testUUIDConversions(HeaderPrintWriter out, String uuidstring)
	{
		UUID uuid = factory.recreateUUID(uuidstring);
		if (!uuidstring.equalsIgnoreCase(uuid.toString())){
			// Resolve: format this with a message factory
			String message = 
				"Couldn't recreate UUID String: "
				+ uuidstring 
				+ " != " 
				+ uuid.toString();
			out.printlnWithHeader(message);
			resultSoFar =  false;
		}
	}
}
