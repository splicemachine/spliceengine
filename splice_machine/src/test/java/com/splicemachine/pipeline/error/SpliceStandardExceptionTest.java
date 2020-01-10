/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.pipeline.error;

import com.splicemachine.pipeline.SpliceStandardException;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import com.splicemachine.db.iapi.error.StandardException;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ArchitectureIndependent.class)
public class SpliceStandardExceptionTest {
	@Test 
	public void instanceTest() {
		StandardException se = StandardException.unexpectedUserException(new Exception("Unexpected"));
		SpliceStandardException sse = new SpliceStandardException(se);
		Assert.assertEquals(sse.getSeverity(), se.getSeverity());
		Assert.assertEquals(sse.getSqlState(), se.getSqlState());
		Assert.assertEquals(sse.getTextMessage(), se.getTextMessage());
	}

	@Test 
	public void generateStandardExceptionTest() {
		StandardException se = StandardException.unexpectedUserException(new Exception("Unexpected"));
		SpliceStandardException sse = new SpliceStandardException(se);
		StandardException se2 = sse.generateStandardException();
		Assert.assertEquals(se2.getSeverity(), se.getSeverity());
		Assert.assertEquals(se2.getSqlState(), se.getSqlState());
		Assert.assertEquals(se2.getTextMessage(), se.getTextMessage());
	}

	
}
