package com.splicemachine.db.iapi.sql.compile;

/**
 * Compilation phases for tree handling
 */
public enum CompilationPhase {

    // derby used to represent with integer 0
    AFTER_PARSE,

    // derby used to represent with integer 1
    AFTER_BIND,

    // derby used to represent with integer 2
    AFTER_OPTIMIZE
}
