/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.impl.rollforward;

import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.utils.ByteSlice;

/**
 * @author Scott Fines
 *         Date: 7/1/14
 */
public class NoopRollForward implements RollForward{
		public static final RollForward INSTANCE = new NoopRollForward();

		private NoopRollForward(){}

	@Override public void submitForResolution(ByteSlice rowKey, long txnId) {  }
		@Override public void recordResolved(ByteSlice rowKey, long txnId) {  }

}
