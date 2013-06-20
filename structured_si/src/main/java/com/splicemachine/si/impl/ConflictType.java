package com.splicemachine.si.impl;

/**
 * Indicates whether/how two transaction's writes would or do conflict with each other.
 */
public enum ConflictType {
    NONE, // the two transactions do not and would not conflict
    SIBLING, // the two transactions are effectively siblings of each other, meaning one does not contain the other, but they would or do conflict
    CHILD // one transaction is a descendant of the other and they would or do conflict
}
