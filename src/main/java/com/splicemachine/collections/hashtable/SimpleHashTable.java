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
    protected int hash(K key) {
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
