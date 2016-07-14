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

package com.splicemachine.stats;

/**
 * a float-specific data streaming data structure.
 *
 * This is functionally equivalent to {@link com.splicemachine.stats.Updateable<Float>},
 * but supports additional methods to avoid extraneous object creation due to autoboxing.
 *
 * @author Scott Fines
 *         Date: 3/26/14
 */
public interface FloatUpdateable extends Updateable<Float>{

		/**
		 * Update the underlying data structure with the new item.
		 *
		 * This is functionally equivalent to {@link #update(Object)}, but avoids
		 * autoboxing object creation.
		 *
		 * @param item the new item to update
		 */
		void update(float item);

		/**
		 * Update the underlying data structure with the new item,
		 * assuming that the new item occurs {@code count} number of times.
		 *
		 * This is functionally equivalent to {@link #update(Object,long)}, but avoids
		 * autoboxing object creation.
		 *
		 * @param item the new item to update
		 * @param count the number of times the item occurs in the stream.
		 */
		void update(float item, long count);
}
