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

package com.splicemachine.collections.hashtable;

/**
 * A simple implementation of a HashTable which is like a HashMap, but uses RobinHood hashing
 * with open addressing instead of linking entries.
 *
 * @author Scott Fines
 *         Date: 10/8/14
 */
public class SimpleHashTable<K,V> extends BaseRobinHoodHashTable<K,V> {
    private static final int DEFAULT_INITIAL_CAPACITY = 1<<4;
    private static final float DEFAULT_LOAD_FACTOR = 0.9f; //use a high load factor, cause we use RH hashing

    public SimpleHashTable() {
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public SimpleHashTable(int initialSize) {
        super(initialSize, DEFAULT_LOAD_FACTOR);
    }

    public SimpleHashTable(float loadFactor) {
        super(DEFAULT_INITIAL_CAPACITY, loadFactor);
    }

    public SimpleHashTable(int initialSize, float loadFactor) {
        super(initialSize, loadFactor);
    }

    @Override
    protected int hashCode(K key) {
        int h = key.hashCode();
        /*
         * Taken from java.util.HashMap. This smears out the hashCode in an attempt
         * to reduce spikes due to poorly constructed hash codes.
         */
        h ^= (h>>>20)^(h>>>12);
        return h ^(h>>>7)^(h>>>4);
    }

    @Override
    protected V merge(V newValue, V existing) {
        return newValue;
    }
}
