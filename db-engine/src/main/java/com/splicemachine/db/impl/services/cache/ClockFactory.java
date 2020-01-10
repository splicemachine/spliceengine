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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.services.cache;

import com.splicemachine.db.iapi.services.cache.CacheFactory;
import com.splicemachine.db.iapi.services.cache.CacheManager;
import com.splicemachine.db.iapi.services.cache.CacheableFactory;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

/**
  Multithreading considerations: no need to be MT-safe, caller (module control)
  provides synchronization. Besides, this class is stateless.
*/

public class ClockFactory implements CacheFactory {

	/**
		Trace flag to display cache statistics
	*/
	public static final String CacheTrace = SanityManager.DEBUG ? "CacheTrace" : null;

	public ClockFactory() {
	}



	/*
	** Methods of CacheFactory
	*/

	public CacheManager newCacheManager(CacheableFactory holderFactory, String name, int initialSize, int maximumSize)
	{

		if (initialSize <= 0)
			initialSize = 1;

		return new Clock(holderFactory, name, initialSize, maximumSize, false);
	}
	
	public CacheManager newSizedCacheManager(CacheableFactory holderFactory, String name,
										int initialSize, long maximumSize)
	{

		if (initialSize <= 0)
			initialSize = 1;

		return new Clock(holderFactory, name, initialSize, maximumSize, true);
	}
}
