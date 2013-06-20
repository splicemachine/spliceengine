package com.splicemachine.si.impl;

/**
 * Same as TransactionSchema except the fields are in their encoded form.
 */
public class EncodedTransactionSchema<Data> {
    final String tableName;
    final Data siFamily;
    final Data siNull;

    final Data idQualifier;
    final Data startQualifier;
    final Data parentQualifier;
    final Data dependentQualifier;
    final Data allowWritesQualifier;
    final Data readUncommittedQualifier;
    final Data readCommittedQualifier;
    final Data commitQualifier;
    final Data globalCommitQualifier;
    final Data statusQualifier;
    final Data keepAliveQualifier;
    final Data counterQualifier;

    public EncodedTransactionSchema(String tableName, Data siFamily, Data siNull, Data idQualifier, Data startQualifier,
                                    Data parentQualifier, Data dependentQualifier, Data allowWritesQualifier,
                                    Data readUncommittedQualifier, Data readCommittedQualifier, Data keepAliveQualifier,
                                    Data statusQualifier, Data commitQualifier, Data globalCommitQualifier,
                                    Data counterQualifier) {
        this.tableName = tableName;
        this.siFamily = siFamily;
        this.siNull = siNull;

        this.idQualifier = idQualifier;
        this.startQualifier = startQualifier;
        this.parentQualifier = parentQualifier;
        this.dependentQualifier = dependentQualifier;
        this.allowWritesQualifier = allowWritesQualifier;
        this.readUncommittedQualifier = readUncommittedQualifier;
        this.readCommittedQualifier = readCommittedQualifier;
        this.commitQualifier = commitQualifier;
        this.globalCommitQualifier = globalCommitQualifier;
        this.statusQualifier = statusQualifier;
        this.keepAliveQualifier = keepAliveQualifier;
        this.counterQualifier = counterQualifier;
    }
}