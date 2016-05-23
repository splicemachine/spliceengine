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
    AFTER_OPTIMIZE,

    // Derby didn't have an constant for this phase. However 'generate' was always called as the 4th step of
    // GenericStatement.fourPhasePrepare() -- splice currently uses this constant only to generate AST visualization
    // post generate (yes, tree can change significantly in the generate phase).
    AFTER_GENERATE
}
