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

package com.splicemachine.dbTesting.unitTests.lang;

import com.splicemachine.dbTesting.unitTests.harness.T_Generic;
import com.splicemachine.dbTesting.unitTests.harness.T_Fail;

import com.splicemachine.db.iapi.types.*;

import com.splicemachine.db.iapi.reference.Property;

import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;

import com.splicemachine.db.iapi.error.StandardException;

import java.util.Properties;

/**
	@see Like

 */
public class T_Like extends T_Generic
{
	private static final String testService = "likeTest";
	boolean didFAIL;

	/*
	** Methods required by T_Generic
	*/

	public String getModuleToTestProtocolName() {
		// actually, we're just testing LIKE; but it is reached through
		// the ExecutionFactory MODULE, and this wants a MODULE, so...
		return ExecutionFactory.MODULE;
	}

	/**
		@exception T_Fail test failed.
	*/
	protected void runTests() throws T_Fail
	{
		ExecutionFactory f = null;
		boolean pass = false;
		didFAIL = false;

        out.println(testService+" underway");

		// don't automatic boot this service if it gets left around
		if (startParams == null) {
			startParams = new Properties();
		}
		startParams.put(Property.NO_AUTO_BOOT, Boolean.TRUE.toString());

		REPORT("(unitTestMain) Testing " + testService);

		// REMIND: add tests here
		try {
			tests();
		} catch (StandardException e) {
			FAIL("exception:"+e);
		}
		if (didFAIL) throw T_Fail.testFailMsg("see log for details");

        out.println(testService+" complete");
	}

	// testing support:
	private void expect(String desc, Boolean test, Boolean result) {
		boolean pass =
			( (test == null && result == null) ||
			  (test != null && result != null && test.equals(result)) );

		if (pass)
			PASS("TEST ["+desc+"] == result["+result+"] ");
		else
			FAIL("TEST ["+desc+"] != result["+result+"] ");
	}

	// testing mechanism:
	private void tests() throws StandardException {
		boolean gotLE = false;
		Boolean t = null;
		char[] caNull = null;
		char[] caHello = "hello".toCharArray();
		String msg=null;
		String desc=null;

		REPORT("testing null combinations...");
		try {
		expect("null like null escape null", Like.like(caNull, 0, caNull, 0, caNull, 0, null), null);
		expect("null like 'hello' escape null", Like.like(caNull, 0, caHello, caHello.length, caNull, 0, null), null);
		expect("'hello' like null escape null", Like.like(caHello, caHello.length, caNull, 0, caNull, 0, null), null);
		expect("null like null escape '\\'", Like.like(caNull, 0, caNull, 0, "\\".toCharArray(), "\\".toCharArray().length, null), null);

		// gets back a null before it evaluates the escape
		expect("null like null escape 'hello'", Like.like(caNull, 0, caNull, 0, caHello, caHello.length, null), null);
		// gets back a null before it evaluates the pattern
		expect("null like 'hello\\' escape '\\'", Like.like(caNull, 0, "hello\\".toCharArray(), "hello\\".toCharArray().length, "\\".toCharArray(), "\\".toCharArray().length, null), null);

		} catch(StandardException leOuter1) {
			leOuter1.printStackTrace();
			FAIL("unexpected exception");
		}

		REPORT("testing valid match cases...");
		try {
		expect("'hello' like 'hello' escape null", Like.like(caHello, caHello.length, caHello, caHello.length, caNull, 0, null), Boolean.TRUE);
		expect("'hello' like 'h_llo' escape null", Like.like(caHello, caHello.length, "h_llo".toCharArray(), "h_llo".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'hello' like '_ello' escape null", Like.like(caHello, caHello.length, "_ello".toCharArray(), "_ello".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'hello' like 'hell_' escape null", Like.like(caHello, caHello.length, "hell_".toCharArray(), "hell_".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'hello' like '_____' escape null", Like.like(caHello, caHello.length, "_____".toCharArray(), "_____".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'hello' like 'h___e' escape null", Like.like(caHello, caHello.length, "h___o".toCharArray(), "h___o".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'h' like 'h' escape null", Like.like("h".toCharArray(), "h".toCharArray().length, "h".toCharArray(), "h".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'h' like 'F' escape null", Like.like("h".toCharArray(), "h".toCharArray().length, "F".toCharArray(), "F".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'h' like '%' escape null", Like.like("h".toCharArray(), "h".toCharArray().length, "%".toCharArray(), "%".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'h' like 'F%' escape null", Like.like("h".toCharArray(), "h".toCharArray().length, "F%".toCharArray(), "F%".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'h' like '%F' escape null", Like.like("h".toCharArray(), "h".toCharArray().length, "%F".toCharArray(), "%F".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'h' like '%' escape null", Like.like("h".toCharArray(), "h".toCharArray().length, "%".toCharArray(), "%".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'' like '%' escape null", Like.like("".toCharArray(), "".toCharArray().length, "%".toCharArray(), "%".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'' like '%%' escape null", Like.like("".toCharArray(), "".toCharArray().length, "%%".toCharArray(), "%%".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		expect("'' like '%%%' escape null", Like.like("".toCharArray(), "".toCharArray().length, "%%%".toCharArray(), "%%%".toCharArray().length, caNull, 0, null), Boolean.TRUE);
		} catch(StandardException leOuter2) {
			leOuter2.printStackTrace();
			FAIL("unexpected exception");
		}

		REPORT("testing valid nonmatch cases...");
		try {
		expect("'hello' like 'hello ' escape null", Like.like(caHello, caHello.length, "hello ".toCharArray(), "hello ".toCharArray().length, caNull, 0, null), Boolean.FALSE);
		expect("'hello ' like 'hello' escape null", Like.like("hello ".toCharArray(), "hello ".toCharArray().length, caHello, caHello.length, caNull, 0, null), Boolean.FALSE);
		expect("'hello' like 'hellox' escape null", Like.like(caHello, caHello.length, "hellox".toCharArray(), "hellox".toCharArray().length, caNull, 0, null), Boolean.FALSE);
		expect("'hellox' like 'hello' escape null", Like.like("hellox".toCharArray(), "hellox".toCharArray().length, caHello, caHello.length, caNull, 0, null), Boolean.FALSE);
		expect("'xhellox' like 'hello' escape null", Like.like("xhellox".toCharArray(), "xhellox".toCharArray().length, caHello, caHello.length, caNull, 0, null), Boolean.FALSE);
		expect("'hello' like 'xhellox' escape null", Like.like(caHello, caHello.length, "xhellox".toCharArray(), "xhellox".toCharArray().length, null, 0, null), Boolean.FALSE);
		expect("'hello' like 'h___' escape null", Like.like(caHello, caHello.length, "h___".toCharArray(), "h___".toCharArray().length, caNull, 0, null), Boolean.FALSE);
		expect("'h' like 'F%F' escape null", Like.like("h".toCharArray(), "h".toCharArray().length, "F%F".toCharArray(), "F%F".toCharArray().length, caNull, 0, null), Boolean.FALSE);
		expect("'' like 'F' escape null", Like.like("".toCharArray(), "".toCharArray().length, "F".toCharArray(), "F".toCharArray().length, caNull, 0, null), Boolean.FALSE);
		} catch(StandardException leOuter3) {
			leOuter3.printStackTrace();
			FAIL("unexpected exception");
		}

		REPORT("testing error cases...");

		try {
			msg = null;
			gotLE=false;
			desc="null like null escape 'hello'";
			t=Like.like(caHello, caHello.length, caHello, caHello.length, caHello, caHello.length, null);
		} catch (StandardException le) {
			gotLE=true;
			msg = le.getMessage();
		} finally {
			if (gotLE)
				PASS("TEST ["+desc+"] got exception "+msg);
			else
				FAIL("TEST ["+desc+"] didn't get exception");
		}

		try {
			msg = null;
			gotLE=false;
			desc="'hello' like 'hhh' escape 'h'";
			t=Like.like(caHello, caHello.length, "hhh".toCharArray(), "hhh".toCharArray().length, "h".toCharArray(), "h".toCharArray().length, null);
		} catch (StandardException le) {
			gotLE=true;
			msg = le.getMessage();
		} finally {
			if (gotLE)
				PASS("TEST ["+desc+"] got exception "+msg);
			else
				FAIL("TEST ["+desc+"] didn't get exception");
		}

		try {
			msg = null;
			gotLE=false;
			desc="'hello' like 'he%' escape 'h'";
			t=Like.like(caHello, caHello.length, "he%".toCharArray(), "he%".toCharArray().length, "h".toCharArray(), "h".toCharArray().length, null);
		} catch (StandardException le) {
			gotLE=true;
			msg = le.getMessage();
		} finally {
			if (gotLE)
				PASS("TEST ["+desc+"] got exception "+msg);
			else
				FAIL("TEST ["+desc+"] didn't get exception");
		}

	}

	/*
		override to mark the test as failed when dumping the message.
	 */
	protected boolean FAIL(String msg) {
		super.FAIL(msg);
		return didFAIL = true;
	}
}

