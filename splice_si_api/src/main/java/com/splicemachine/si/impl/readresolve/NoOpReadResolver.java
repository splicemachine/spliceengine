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

package com.splicemachine.si.impl.readresolve;

import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.utils.ByteSlice;

/**
 * @author Scott Fines
 *         Date: 6/26/14
 */
public class NoOpReadResolver implements ReadResolver {
		public static final ReadResolver INSTANCE = new NoOpReadResolver();
    @Override public void resolve(ByteSlice rowKey, long txnId) {  }
}
