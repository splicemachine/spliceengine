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

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.math.BigDecimal;

/**
 * outparams30 contains java procedures using java.math.BigDecimal.
 * These are moved to this class to enable tests using other procedures
 * in outparams.java to run in J2ME/CDC/FP.
 *   
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class outparams30 extends outparams {
	
	public static void takesBigDecimal(BigDecimal[] outparam, int type)
	{
		outparam[0] = (outparam[0] == null ? new BigDecimal("33") : outparam[0].add(outparam[0]));
		outparam[0].setScale(4, BigDecimal.ROUND_DOWN);
	}
	
	public static BigDecimal returnsBigDecimal(int type)
	{
		return new BigDecimal(666d);
	}

}
