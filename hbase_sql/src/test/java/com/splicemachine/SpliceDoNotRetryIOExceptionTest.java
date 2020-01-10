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

package com.splicemachine;

import java.io.IOException;

import org.junit.Assert;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.junit.Test;


public class SpliceDoNotRetryIOExceptionTest {
	protected static String msg1 = "CHECK WITH ME";
	protected static String msg2 = "CHECK WITH ME 2";
	protected static SpliceDoNotRetryIOException exception = new SpliceDoNotRetryIOException(msg1,new Exception(msg2));
	@Test
	public void instanceOfTest() {
		Assert.assertTrue(exception instanceof IOException);
		Assert.assertTrue(exception instanceof DoNotRetryIOException);
	}
	@Test
	public void messageTrimmingTest() {
		Assert.assertEquals(msg1,exception.getMessage());
	}
	
}
