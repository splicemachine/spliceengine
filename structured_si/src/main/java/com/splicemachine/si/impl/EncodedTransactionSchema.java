package com.splicemachine.si.impl;

/**
 * Same as TransactionSchema except the fields are in their encoded form.
 */
public class EncodedTransactionSchema {
    final String tableName;
    public final byte[] siFamily;
    public final byte[] permissionFamily;
    public final byte[] siNull;

    public final byte[] idQualifier;
    public final byte[] startQualifier;
    public final byte[] parentQualifier;
    public final byte[] dependentQualifier;
    public final byte[] allowWritesQualifier;
    public final byte[] additiveQualifier;
    public final byte[] readUncommittedQualifier;
    public final byte[] readCommittedQualifier;
    public final byte[] commitQualifier;
    public final byte[] globalCommitQualifier;
    public final byte[] statusQualifier;
    public final byte[] keepAliveQualifier;
    public final byte[] counterQualifier;
		public final byte[] writeTableQualifier;

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
                                    byte[] counterQualifier,
																		byte[] writeTableQualifier) {
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
				this.writeTableQualifier = writeTableQualifier;
    }
}