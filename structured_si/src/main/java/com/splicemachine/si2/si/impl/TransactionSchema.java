package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.data.api.SDataLib;

public class TransactionSchema {
    final String relationIdentifier;
    final Object siFamily;
    final Object startQualifier;
    final Object commitQualifier;
    final Object statusQualifier;

    public TransactionSchema(String relationIdentifier, Object siFamily, Object startQualifier, Object commitQualifier,
                             Object statusQualifier) {
        this.relationIdentifier = relationIdentifier;
        this.siFamily = siFamily;
        this.startQualifier = startQualifier;
        this.commitQualifier = commitQualifier;
        this.statusQualifier = statusQualifier;
    }

    public TransactionSchema encodedSchema(SDataLib SDataLib) {
        return new TransactionSchema(relationIdentifier,
                SDataLib.encode((String) siFamily),
                SDataLib.encode(startQualifier),
                SDataLib.encode(commitQualifier),
                SDataLib.encode(statusQualifier));
    }
}
