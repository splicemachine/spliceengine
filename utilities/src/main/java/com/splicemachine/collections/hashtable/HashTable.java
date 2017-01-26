/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.collections.hashtable;

import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 10/8/14
 */
public interface HashTable<K,V> extends Map<K,V> {


    /**
     * Remove the entry associated with the specified key.
     *
     * @param key the key to remove the value for
     * @param forceRemoveValue if true, then forcibly clean the underlying data structure;
     *                         otherwise, the implementation may decide not to dereference the
     *                         stored objects, which may cause excessive memory usage when a
     *                         large number of deletions occur.
     * @return the value previously associated with the specified key, or {@code null} if
     * no entry with that key exists.
     */
    V remove(K key, boolean forceRemoveValue);

    /**
     * Some implementations may lazily remove entries, which would leave an
     * element behind in the hashtable. This can cause confusion with memory (and
     * potentially memory problems if a large number of deletions occur). By specifying
     * {@code forceRemoveValues = true}, the implementation should forcibly remove entries,
     * and thus improve overall memory usage (generally at a performance penalty).
     *
     * @param forceRemoveValues
     */
    void clear(boolean forceRemoveValues);

    /**
     * @return the ratio of filled entries to available entries. Mostly useful for debugging
     * and other monitoring.
     */
    float load();
}
