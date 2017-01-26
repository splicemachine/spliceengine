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

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Interface that must be implemented by classes that provide a replacement
 * algorithm for <code>ConcurrentCache</code>.
 */
interface ReplacementPolicy {
    /**
     * Insert an entry into the <code>ReplacementPolicy</code>'s data
     * structure, possibly evicting another entry. The entry should be
     * uninitialized when the method is called (that is, its
     * <code>Cacheable</code> should be <code>null</code>), and it should be
     * locked. When the method returns, the entry may have been initialized
     * with a <code>Cacheable</code> which is ready to be reused. It is also
     * possible that the <code>Cacheable</code> is still <code>null</code> when
     * the method returns, in which case the caller must allocate one itself.
     * The entry will be associated with a {@code Callback} object that it can
     * use to communicate back to the replacement policy events (for instance,
     * that it has been accessed or become invalid).
     *
     * @param entry the entry to insert
     * @exception StandardException if an error occurs while inserting the
     * entry
     *
     * @see CacheEntry#setCallback(ReplacementPolicy.Callback)
     */
    void insertEntry(CacheEntry entry) throws StandardException;

    /**
     * Try to shrink the cache if it has exceeded its maximum size. It is not
     * guaranteed that the cache will actually shrink.
     */
    void doShrink();

    /**
     * The interface for the callback objects that <code>ConcurrentCache</code>
     * uses to notify the replacement algorithm about events such as look-ups
     * and removals. Each <code>Callback</code> object is associated with a
     * single entry in the cache.
     */
    interface Callback {
        /**
         * Notify the replacement algorithm that the cache entry has been
         * accessed. The replacement algorithm can use this information to
         * collect statistics about access frequency which can be used to
         * determine the order of evictions.
         *
         * <p>
         *
         * The entry associated with the callback object must be locked by the
         * current thread.
         */
        void access();

        /**
         * Notify the replacement algorithm that the entry associated with this
         * callback object has been removed, and the callback object and the
         * <code>Cacheable</code> can be reused.
         *
         * <p>
         *
         * The entry associated with the callback object must be locked by the
         * current thread.
         */
        void free();
    }
}
