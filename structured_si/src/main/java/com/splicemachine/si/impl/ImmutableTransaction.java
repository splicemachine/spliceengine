package com.splicemachine.si.impl;

import com.splicemachine.si.api.TransactionId;

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
    final TransactionBehavior behavior;
    private final TransactionStore transactionStore;

    private final SITransactionId transactionId;
    private final ImmutableTransaction immutableParent;
    private final boolean dependent;
    private final boolean allowWrites;

    private final long beginTimestamp;
    private final Boolean readUncommitted;
    private final Boolean readCommitted;

    public ImmutableTransaction(TransactionBehavior behavior, TransactionStore transactionStore,
                                SITransactionId transactionId, boolean allowWrites, Boolean readCommitted,
                                ImmutableTransaction immutableParent, boolean dependent, Boolean readUncommitted,
                                long beginTimestamp) {
        this.behavior = behavior;
        this.transactionStore = transactionStore;
        this.transactionId = transactionId;
        this.immutableParent = immutableParent;
        this.dependent = dependent;
        this.allowWrites = allowWrites;
        this.beginTimestamp = beginTimestamp;
        this.readUncommitted = readUncommitted;
        this.readCommitted = readCommitted;
    }

    public ImmutableTransaction(TransactionBehavior behavior, TransactionStore transactionStore, long id,
                                boolean allowWrites, Boolean readCommitted, ImmutableTransaction immutableParent,
                                boolean dependent, Boolean readUncommitted,
                                long beginTimestamp) {
        this(behavior, transactionStore, new SITransactionId(id), allowWrites, readCommitted, immutableParent,
                dependent, readUncommitted, beginTimestamp);
    }

    public long getBeginTimestampDirect() {
        return beginTimestamp;
    }

    public SITransactionId getTransactionId() {
        return transactionId;
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
        return isDescendant(transactionStore.getImmutableTransaction(transactionId2));
    }

    public boolean isDescendant(ImmutableTransaction t2) throws IOException {
        final ImmutableTransaction t1 = this;
        final ImmutableTransaction[] intersections = intersect(false, t1.getChain(), t2.getChain());
        final ImmutableTransaction et2 = intersections[2];
        return et2.immutableParent.sameTransaction(t1);
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
        final TransactionStatus effectiveStatus2 = t2.getEffectiveStatus(et2);
        if (et1.sameTransaction(et2)) {
            visible = true;
        } else {
            if (!readCommitted1 && !readUncommitted1 && effectiveStatus2.equals(COMMITTED)) {
                visible = (met2.getCommitTimestamp(met2.getParent()) < et1.getBeginTimestampDirect())
                        || et1.isDescendant(et2);
            }
            if (effectiveStatus2.equals(ACTIVE) && et1.immutableParent.sameTransaction(et2)) {
                visible = true;
            }
            if (readCommitted1 && effectiveStatus2.equals(COMMITTED)) {
                visible = true;
            }
            if (readUncommitted1 && effectiveStatus2.equals(ACTIVE)) {
                visible = true;
            }
        }
        return new Object[]{visible, effectiveStatus2};
    }

    public ConflictType isConflict(ImmutableTransaction t2) throws IOException {
        final ImmutableTransaction t1 = this;
        final ImmutableTransaction[] intersections = intersect(true, t1.getChain(), t2.getChain());
        final ImmutableTransaction shared = intersections[0];
        final ImmutableTransaction et1 = intersections[1];
        final ImmutableTransaction et2 = intersections[2];
        final Transaction met2 = transactionStore.getTransaction(et2.getTransactionId().getId());
        if (met2.getStatus().equals(COMMITTED)) {
            if (et1.isDescendant(et2)) {
                return ConflictType.CHILD;
            }
            return (met2.getCommitTimestamp(shared) > et1.getBeginTimestampDirect()) &&
                    !et1.isDescendant(et2) ? ConflictType.SIBLING : ConflictType.NONE;
        } else if (met2.getStatus().equals(ACTIVE)) {
            if (et1.sameTransaction(et2)
                    || et1.immutableParent.sameTransaction(et2)) {
                return ConflictType.NONE;
            } else if (et1.isDescendant(et2)) {
                return ConflictType.CHILD;
            } else {
                return ConflictType.SIBLING;
            }
        } else {
            return ConflictType.NONE;
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

    public ImmutableTransaction[] intersect(boolean collapse, List<ImmutableTransaction> chain1, List<ImmutableTransaction> chain2) {
        for (int i2 = 0; i2 < chain2.size(); i2++) {
            for (int i1 = 0; i1 < chain1.size(); i1++) {
                if (chain1.get(i1).sameTransaction(chain2.get(i2))) {
                    final ImmutableTransaction t2 = chain2.get(0);
                    final ImmutableTransaction et1 = chain1.get(i1 == 0 ? 0 : i1 - 1);
                    ImmutableTransaction et2 = chain2.get(i2 == 0 ? 0 : i2 - 1);
                    if (collapse && t2.behavior.collapsible()) {
                        et2 = t2;
                    }
                    return new ImmutableTransaction[]{chain1.get(i1), et1, et2};
                }
            }
        }
        return null;
    }

    public long getBeginTimestamp() {
        return getBeginTimestamp(Transaction.getRootTransaction());
    }

    public long getBeginTimestamp(ImmutableTransaction scope) {
        if (immutableParent.sameTransaction(scope)) {
            return beginTimestamp;
        } else {
            return immutableParent.getBeginTimestamp();
        }
    }

    public boolean sameTransaction(ImmutableTransaction other) {
        return (getTransactionId().getId() == other.getTransactionId().getId()
                && !getTransactionId().independentReadOnly
                && !other.getTransactionId().independentReadOnly);
    }

    @Override
    public String toString() {
        return "ImmutableTransaction: " + transactionId;
    }

    public ImmutableTransaction cloneWithId(TransactionId newTransactionId, ImmutableTransaction parent) {
        if (transactionId.getId() != newTransactionId.getId()) {
            throw new RuntimeException("Cannot clone transaction with different id");
        }
        return new ImmutableTransaction(behavior, transactionStore, (SITransactionId) newTransactionId,
                allowWrites, readCommitted, parent, dependent, readUncommitted, beginTimestamp);
    }

}
