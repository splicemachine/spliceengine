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
 * Defines an algorithm which can be updated.
 *
 * This interface is an object-representation of the elements--each individual
 * java primitive (int, short, etc.) has a subinterface which allows for efficient,
 * non-autoboxed versions of this interface.
 *
 * @author Scott Fines
 * Date: 6/5/14
 */
public interface Updateable<T> {

    /**
     * Update the underlying data structure with the new item.
     *
     * @param item the new item to update
     */
		public void update(T item);

    /**
     * Update the underlying data structure with the new item,
     * assuming that the new item occurs {@code count} number of times.
     *
     * @param item the new item to update
     * @param count the number of times the item occurs in the stream.
     */
		public void update(T item, long count);
}
