package com.splicemachine.si.impl;

/**
 * Considering key-values from data tables, based on the column family and qualifier they can be classified as one of
 * these types.
 */
public enum KeyValueType {
    COMMIT_TIMESTAMP,
    TOMBSTONE,
    USER_DATA,
    OTHER
}
