package com.splicemachine.si.impl;

/**
 * Holder objects for the arguments to the permission check method. This object serves as the key in the map used to
 * memoize the method.
 */
public class PermissionArgs {
    final TransactionId transactionId;
    final String tableName;

    public PermissionArgs(TransactionId transactionId, String tableName) {
        this.transactionId = transactionId;
        this.tableName = tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PermissionArgs that = (PermissionArgs) o;

        if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) return false;
        if (transactionId != null ? !transactionId.equals(that.transactionId) : that.transactionId != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = transactionId != null ? transactionId.hashCode() : 0;
        result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
        return result;
    }
}
