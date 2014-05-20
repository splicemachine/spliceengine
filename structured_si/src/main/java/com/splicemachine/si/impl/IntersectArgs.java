package com.splicemachine.si.impl;

/**
 * The arguments passed into the intersect method. This object is used as a key in the cache that memoizes the method.
 */
public class IntersectArgs {
    final boolean collapse;
    final ImmutableTransaction transaction1;
    final ImmutableTransaction transaction2;

    public IntersectArgs(boolean collapse, ImmutableTransaction transaction1, ImmutableTransaction transaction2) {
        this.collapse = collapse;
        this.transaction1 = transaction1;
        this.transaction2 = transaction2;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;

        IntersectArgs that = (IntersectArgs) o;

        if (transaction1.getLongTransactionId() != that.transaction1.getLongTransactionId()) return false;
        if (transaction2.getLongTransactionId() != that.transaction2.getLongTransactionId()) return false;
        return true;
    }

    @Override
    public int hashCode() {
    	long x = transaction1.getLongTransactionId();
    	long y = transaction2.getLongTransactionId();
        int result = (int) (x ^ (x >>> 32));
        result = 31 * result + (int) (y ^ (y >>> 32));
        return result;
    }
    
}
