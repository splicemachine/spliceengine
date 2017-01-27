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

package com.splicemachine.db.impl.services.cache;

class CacheStat {

	/*
	** Fields
	*/
	protected int findHit;
	protected int findMiss;
	protected int findFault;
	protected int findCachedHit;
	protected int findCachedMiss;
	protected int create;
	protected int ageOut;
	protected int cleanAll;
	protected int remove;
	protected long initialSize;
	protected long maxSize;
	protected long currentSize;

	protected long[] data;

	public long[] getStats() 
	{
		if (data == null)
			data = new long[14];

		data[0] = findHit + findMiss;
		data[1] = findHit;
		data[2] = findMiss;
		data[3] = findFault;
		data[4] = findCachedHit + findCachedMiss;
		data[5] = findCachedHit;
		data[6] = findCachedMiss;
		data[7] = create;
		data[8] = ageOut;
		data[9] = cleanAll;
		data[10] = remove;
		data[11] = initialSize;
		data[12] = maxSize;
		data[13] = currentSize;

		return data;
	}

	public void reset()
	{
		findHit = 0;
		findMiss = 0;
		findFault = 0;
		findCachedHit = 0;
		findCachedMiss = 0;
		create = 0;
		ageOut = 0;
		cleanAll = 0;
		remove = 0;
	}
}
