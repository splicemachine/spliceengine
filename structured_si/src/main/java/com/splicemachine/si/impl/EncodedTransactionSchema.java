package com.splicemachine.si.impl;

/**
 * Same as TransactionSchema except the fields are in their encoded form.
 */
public class EncodedTransactionSchema {
    final String tableName;
    final byte[] siFamily;
    final byte[] permissionFamily;
    final byte[] siNull;

    final byte[] idQualifier;
    final byte[] startQualifier;
    final byte[] parentQualifier;
    final byte[] dependentQualifier;
    final byte[] allowWritesQualifier;
    final byte[] additiveQualifier;
    final byte[] readUncommittedQualifier;
    final byte[] readCommittedQualifier;
    final byte[] commitQualifier;
    final byte[] globalCommitQualifier;
    final byte[] statusQualifier;
    final byte[] keepAliveQualifier;
    final byte[] counterQualifier;

    public EncodedTransactionSchema(String tableName,
																		byte[] siFamily,
																		byte[] permissionFamily,
																		byte[] siNull,
																		byte[] idQualifier,
                                    byte[] startQualifier,
																		byte[] parentQualifier,
																		byte[] dependentQualifier,
																		byte[] allowWritesQualifier,
                                    byte[] additiveQualifier,
																		byte[] readUncommittedQualifier,
																		byte[] readCommittedQualifier,
                                    byte[] keepAliveQualifier,
																		byte[] statusQualifier,
																		byte[] commitQualifier,
																		byte[] globalCommitQualifier,
                                    byte[] counterQualifier) {
        this.tableName = tableName;
        this.siFamily = siFamily;
        this.permissionFamily = permissionFamily;
        this.siNull = siNull;

        this.idQualifier = idQualifier;
        this.startQualifier = startQualifier;
        this.parentQualifier = parentQualifier;
        this.dependentQualifier = dependentQualifier;
        this.allowWritesQualifier = allowWritesQualifier;
        this.additiveQualifier = additiveQualifier;
        this.readUncommittedQualifier = readUncommittedQualifier;
        this.readCommittedQualifier = readCommittedQualifier;
        this.commitQualifier = commitQualifier;
        this.globalCommitQualifier = globalCommitQualifier;
        this.statusQualifier = statusQualifier;
        this.keepAliveQualifier = keepAliveQualifier;
        this.counterQualifier = counterQualifier;
    }
}