package com.splicemachine.derby.impl.sql.execute.actions;

/**
 * Provider of a name to be used as a displayable short description
 * of an operation or sub operation within a running or completed query.
 * For example, sub types of {@link SpliceBaseOperation} or implementors
 * of {@link ConstantAction} will typically provide a name that can be
 * used in the Spark job admin pages to identify the operation
 * within a job or a stage.
 */
public interface ScopeNamed {
    String getScopeName();
}
