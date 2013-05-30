package com.splicemachine.si.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.splicemachine.si.impl.TransactionStatus.ACTIVE;
import static com.splicemachine.si.impl.TransactionStatus.COMMITTED;

/**
 * Represents the parts of a transaction record that do not change once the transaction is created. It is useful to
 * represent these immutable parts separately because they can be cached more aggressively.
 */
public class ImmutableTransaction {
    private final TransactionStore transactionStore;

    private final SITransactionId transactionId;
    private final ImmutableTransaction immutableParent;
    private final boolean dependent;
    private final boolean allowWrites;

    private final long beginTimestamp;
    final Boolean readUncommitted;
    final Boolean readCommitted;

    public ImmutableTransaction(TransactionStore transactionStore, long id, boolean allowWrites, Boolean readCommitted,
                                ImmutableTransaction immutableParent, boolean dependent, Boolean readUncommitted,
                                long beginTimestamp) {
        this.transactionStore = transactionStore;
        this.transactionId = new SITransactionId(id);
        this.beginTimestamp = beginTimestamp;
        this.readCommitted = readCommitted;
        this.readUncommitted = readUncommitted;
        this.allowWrites = allowWrites;
        this.immutableParent = immutableParent;
        this.dependent = dependent;
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public SITransactionId getTransactionId() {
        return transactionId;
    }

    /**
     * Returns true if this is _not_ a root transaction.
     */
    boolean isNested() {
        return immutableParent != null;
    }

    public boolean isReadOnly() {
        return !allowWrites;
    }

    public boolean getEffectiveReadUncommitted() {
        if (readUncommitted == null) {
            return immutableParent.getEffectiveReadUncommitted();
        }
        return readUncommitted;
    }

    public boolean getEffectiveReadCommitted() {
        if (readCommitted == null) {
            return immutableParent.getEffectiveReadCommitted();
        }
        return readCommitted;
    }

    ////

    public boolean isDescendant(long transactionId2) throws IOException {
        final ImmutableTransaction t1 = this;
        final ImmutableTransaction t2 = transactionStore.getImmutableTransaction(transactionId2);
        final ImmutableTransaction[] intersections = intersect(false, t1.getChain(), t2.getChain());
        final ImmutableTransaction et2 = intersections[2];
        return et2.immutableParent.sameTransaction(t1);
    }

    public boolean isAncestor(long transactionId2) throws IOException {
        final ImmutableTransaction t1 = this;
        final ImmutableTransaction t2 = transactionStore.getImmutableTransaction(transactionId2);
        final ImmutableTransaction[] intersections = intersect(false, t1.getChain(), t2.getChain());
        final ImmutableTransaction et1 = intersections[1];
        return et1.immutableParent.sameTransaction(t2);
    }

    public Object[] isVisible(Transaction t2) throws IOException {
        final ImmutableTransaction t1 = this;
        final ImmutableTransaction[] intersections = intersect(true, t1.getChain(), t2.getChain());
        final ImmutableTransaction et1 = intersections[1];
        final ImmutableTransaction et2 = intersections[2];
        final Transaction met2 = transactionStore.getTransaction(et2.getTransactionId().getId());
        final boolean readCommitted1 = getEffectiveReadCommitted();
        final boolean readUncommitted1 = getEffectiveReadUncommitted();
        boolean visible = false;
        final TransactionStatus effectiveStatus = t2.getEffectiveStatus(et2);
        if (et1.sameTransaction(et2)) {
            visible = true;
        } else {
            if (isIndependent() && isAncestor(t2.getTransactionId().getId())) {
                visible = true;
            } else {
                if (!readCommitted1 && !readUncommitted1 && effectiveStatus.equals(COMMITTED)) {
                    visible = (met2.getCommitTimestamp(met2.getParent()) < et1.getBeginTimestamp())
                            || et2.immutableParent.sameTransaction(et1);
                }
                if (readCommitted1 && effectiveStatus.equals(COMMITTED)) {
                    visible = true;
                }
                if (readUncommitted1 && effectiveStatus.equals(ACTIVE)) {
                    visible = true;
                }
            }
        }
        return new Object[]{visible, effectiveStatus};
    }

    public boolean isConflict(Transaction t2) throws IOException {
        final ImmutableTransaction t1 = this;
        final ImmutableTransaction[] intersections = intersect(true, t1.getChain(), t2.getChain());
        final ImmutableTransaction shared = intersections[0];
        final ImmutableTransaction et1 = intersections[1];
        final ImmutableTransaction et2 = intersections[2];
        final Transaction met2 = transactionStore.getTransaction(et2.getTransactionId().getId());
        if (met2.getStatus().equals(COMMITTED)) {
            return (met2.getCommitTimestamp(shared) > et1.getBeginTimestamp()) &&
                    !et2.immutableParent.sameTransaction(et1);
        } else if (met2.getStatus().equals(ACTIVE)) {
            if (et1.sameTransaction(et2)) {
                return false;
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    public List<ImmutableTransaction> getChain() throws IOException {
        List<ImmutableTransaction> result = new ArrayList<ImmutableTransaction>();
        ImmutableTransaction current = this;
        do {
            result.add(current);
            current = current.immutableParent;
        } while (current != null);
        return Collections.unmodifiableList(result);
    }

    public ImmutableTransaction[] intersect(boolean collapseIndependent, List<ImmutableTransaction> chain1, List<ImmutableTransaction> chain2) {
        for (int i2 = 0; i2 < chain2.size(); i2++) {
            for (int i1 = 0; i1 < chain1.size(); i1++) {
                if (chain1.get(i1).sameTransaction(chain2.get(i2))) {
                    final ImmutableTransaction t2 = chain2.get(0);
                    final ImmutableTransaction et1 = chain1.get(i1 == 0 ? 0 : i1 - 1);
                    ImmutableTransaction et2 = chain2.get(i2 == 0 ? 0 : i2 - 1);
                    if (collapseIndependent && t2.isIndependent() && chain2.get(i2).isRootTransaction()) {
                        et2 = t2;
                    }
                    return new ImmutableTransaction[]{chain1.get(i1), et1, et2};
                }
            }
        }
        return null;
    }

    public long getGlobalBeginTimestamp() {
        if (immutableParent.isRootTransaction()) {
            return beginTimestamp;
        } else {
            return immutableParent.getGlobalBeginTimestamp();
        }
    }

    public boolean isIndependent() {
        return !dependent;
    }

    public boolean isRootTransaction() {
        return sameTransaction(Transaction.getRootTransaction());
    }

    public boolean sameTransaction(ImmutableTransaction other) {
        return getTransactionId().getId() == other.getTransactionId().getId();
    }
}
