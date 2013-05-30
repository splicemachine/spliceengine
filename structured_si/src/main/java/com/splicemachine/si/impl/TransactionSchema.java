package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;

/**
 * Encapsulate the knowledge of the structure of the transaction table.
 */
public class TransactionSchema {
    final String tableName;
    final Object siFamily;
    final Object siChildrenFamily;
    final Object siNull;

    final Object startQualifier;
    final Object parentQualifier;
    final Object dependentQualifier;
    final Object allowWritesQualifier;
    final Object readUncommittedQualifier;
    final Object readCommittedQualifier;
    final Object commitQualifier;
    final Object localCommitQualifier;
    final Object statusQualifier;
    final Object localStatusQualifier;
    final Object keepAliveQualifier;

    public TransactionSchema(String tableName, Object siFamily, Object siChildrenFamily, Object siNull,
                             Object startQualifier, Object parentQualifier,
                             Object dependentQualifier,
                             Object allowWritesQualifier, Object readUncommittedQualifier, Object readCommittedQualifier,
                             Object keepAliveQualifier, Object statusQualifier, Object commitQualifier,
                             Object localStatusQualifier, Object localCommitQualifier) {
        this.tableName = tableName;
        this.siFamily = siFamily;
        this.siChildrenFamily = siChildrenFamily;
        this.siNull = siNull;

        this.startQualifier = startQualifier;
        this.parentQualifier = parentQualifier;
        this.dependentQualifier = dependentQualifier;
        this.allowWritesQualifier = allowWritesQualifier;
        this.readUncommittedQualifier = readUncommittedQualifier;
        this.readCommittedQualifier = readCommittedQualifier;
        this.commitQualifier = commitQualifier;
        this.statusQualifier = statusQualifier;
        this.keepAliveQualifier = keepAliveQualifier;
        this.localCommitQualifier = localCommitQualifier;
        this.localStatusQualifier = localStatusQualifier;
    }

    public TransactionSchema encodedSchema(SDataLib SDataLib) {
        return new TransactionSchema(tableName,
                SDataLib.encode(siFamily),
                SDataLib.encode(siChildrenFamily),
                SDataLib.encode(siNull),
                SDataLib.encode(startQualifier),
                SDataLib.encode(parentQualifier),
                SDataLib.encode(dependentQualifier),
                SDataLib.encode(allowWritesQualifier),
                SDataLib.encode(readUncommittedQualifier),
                SDataLib.encode(readCommittedQualifier),
                SDataLib.encode(keepAliveQualifier), SDataLib.encode(statusQualifier), SDataLib.encode(commitQualifier),
                SDataLib.encode(localStatusQualifier), SDataLib.encode(localCommitQualifier)
        );
    }
}