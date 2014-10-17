package com.splicemachine.si.impl;

/**
 * Indicates whether/how two transaction's writes would or do conflict with each other.
 */
public enum ConflictType {
    NONE, // the two transactions do not and would not conflict
    SIBLING, // the two transactions are effectively siblings of each other, meaning one does not contain the other, but they would or do conflict
    CHILD, // one transaction is a descendant of the other and they would or do conflict
    /**
     * Additive Conflicts occur when two additive transactions interact with one another. Generally,
     * Additive conflicts are ignored (because they don't truly "conflict" in the same way as other conflicts),
     * but occasionally (as in the case of UPSERTs) we want to recognize that we had an additive conflict and
     * deal with it.
     */
    ADDITIVE,
}
