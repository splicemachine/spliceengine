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
 * Boolean-specific implementation of the Updateable interface.
 *
 * This interface is functionaly equivalent to {@link com.splicemachine.stats.Updateable<Boolean>},
 * but supports additional methods so that autoboxing can be avoided whenever possible.
 *
 * @author Scott Fines
 * Date: 3/26/14
 */
public interface BooleanUpdateable extends Updateable<Boolean>{

		/**
		 * Update the underlying data structure with the new item.
		 *
		 * This is a non-autoboxed interface, that allows us to avoid extraneous
		 * object creation during updates.
		 *
		 * @param item the new item to update
		 */
		void update(boolean item);

		/**
		 * Update the underlying data structure with the new item,
		 * assuming that the new item occurs {@code count} number of times.
		 *
		 * This is a non-autoboxed version of {@link #update(Object, long)}, to
		 * avoid extraneous object creation during updates.
		 *
		 * @param item the new item to update
		 * @param count the number of times the item occurs in the stream.
		 */
		void update(boolean item, long count);
}
