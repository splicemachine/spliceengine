package com.splicemachine.si.impl;

import com.splicemachine.si.api.TransactionStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Represents the parts of a transaction record that do not change once the transaction is created. It is useful to
 * represent these immutable parts separately because they can be cached more aggressively.
 */
public class ImmutableTransaction {
    private final static Map<IntersectArgs, ImmutableTransaction[]> intersectCache = CacheMap.makeCache(true);

    final TransactionBehavior behavior;

    private final TransactionId transactionId;
    private final ImmutableTransaction immutableParent;
    private final boolean dependent;
    private final boolean allowWrites;
    private final boolean additive;

    private final long beginTimestamp;
    private final Boolean readUncommitted;
    private final Boolean readCommitted;

    public ImmutableTransaction(TransactionBehavior behavior, TransactionId transactionId, boolean allowWrites,
                                boolean additive, Boolean readCommitted, ImmutableTransaction immutableParent,
                                boolean dependent, Boolean readUncommitted, long beginTimestamp) {
        this.behavior = behavior;
        this.transactionId = transactionId;
        this.immutableParent = immutableParent;
        this.dependent = dependent;
        this.allowWrites = allowWrites;
        this.additive = additive;
        this.beginTimestamp = beginTimestamp;
        this.readUncommitted = readUncommitted;
        this.readCommitted = readCommitted;
    }

    public ImmutableTransaction(TransactionBehavior behavior, long id, boolean allowWrites, boolean additive,
                                Boolean readCommitted, ImmutableTransaction immutableParent, boolean dependent,
                                Boolean readUncommitted, long beginTimestamp) {
        this(behavior, new TransactionId(id), allowWrites, additive, readCommitted, immutableParent,
                dependent, readUncommitted, beginTimestamp);
    }

    public ImmutableTransaction cloneWithId(TransactionId newTransactionId, ImmutableTransaction parent) {
        if (transactionId.getId() != newTransactionId.getId()) {
            throw new RuntimeException("Cannot clone transaction with different id");
        }
        return new ImmutableTransaction(behavior, newTransactionId,
                allowWrites, additive, readCommitted, parent, dependent, readUncommitted, beginTimestamp);
    }

    // immediate access

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public long getLongTransactionId() {
        return transactionId.getId();
    }

    public boolean isReadOnly() {
        return !allowWrites;
    }

    public boolean isAdditive() {
        return additive;
    }

    public boolean isRootTransaction() {
        return transactionId.getId() == Transaction.ROOT_ID;
    }

    public boolean sameTransaction(ImmutableTransaction other) {
        return (getTransactionId().getId() == other.getTransactionId().getId()
                && !getTransactionId().independentReadOnly
                && !other.getTransactionId().independentReadOnly);
    }

    public boolean sameTransaction(long timestamp) {
        return (getTransactionId().getId() == timestamp);
    }

    public ImmutableTransaction getImmutableParent() {
        return immutableParent;
    }

    @Override
    public String toString() {
        return "ImmutableTransaction: " + transactionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ImmutableTransaction that = (ImmutableTransaction) o;

        if (!transactionId.equals(that.transactionId)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return transactionId.hashCode();
    }

    // effective access (see Transaction class for notes on "effective" methods)

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

    public long getEffectiveBeginTimestamp() {
        return getEffectiveBeginTimestamp(Transaction.rootTransaction);
    }

    public long getEffectiveBeginTimestamp(ImmutableTransaction scope) {
        if (immutableParent.sameTransaction(scope)) {
            return beginTimestamp;
        } else {
            return immutableParent.getEffectiveBeginTimestamp();
        }
    }

    // Methods that compute the relationship between transactions. These are the core of the generalized, nested
    // transaction model.

    /**
     * @param t2
     * @return indicator of whether this transaction is an ancestor of t2 in the lineage of nested transactions.
     * @throws IOException
     */
    public boolean isAncestorOf(ImmutableTransaction t2) throws IOException {
        final ImmutableTransaction t1 = this;
        final ImmutableTransaction[] intersections = intersect(false, t1, t2);
        final ImmutableTransaction et2 = intersections[2];
        return et2.immutableParent.sameTransaction(t1);
    }

    /**
		 * Checks if we can see an item which is associated with {@code otherTxn}.
		 *
     * @param otherTxn the transaction to check.
     * @return indicator of whether the writes from otherTxn should be visible from this transaction (based on transaction
     *         status, isolation levels, & timestamps.
     * @throws IOException
     */
    public VisibleResult canSee(Transaction otherTxn, TransactionSource transactionSource) throws IOException {
        final ImmutableTransaction t1 = this;
        final ImmutableTransaction[] intersections = intersect(true, t1, otherTxn);
        final ImmutableTransaction myTransaction = intersections[1];
        final ImmutableTransaction otherTransaction = intersections[2];
        boolean visible = false;
        final TransactionStatus otherTxnEffectiveStatus = otherTxn.getEffectiveStatus(otherTransaction);

				if(myTransaction.sameTransaction(otherTransaction)){
						//we can always see our own writes
						visible=true;
				}else if (otherTxnEffectiveStatus.isActive() && myTransaction.immutableParent.sameTransaction(otherTransaction)) {
						//if the other transaction is active, and I am a child, then it is visible
						visible=true;
				}else{
						boolean isReadCommitted = getEffectiveReadCommitted();
						boolean readUncommitted1 = getEffectiveReadUncommitted();
						Transaction met2 = transactionSource.getTransaction(otherTransaction.getTransactionId().getId());
						if (isReadCommitted && otherTxnEffectiveStatus.isCommitted()) {
								//if I can read committed rows, and this transaction has a known committed state, I can see it
								visible = true;
						} else if (readUncommitted1 && otherTxnEffectiveStatus.isActive()) {
								//if I can read committed rows, and this transaction is active, then I can see it
								visible = true;
						}else if (!isReadCommitted && !readUncommitted1 && otherTxnEffectiveStatus.isCommitted()) {
								/*
								 * We are neither readCommitted (where we could see anything that's been committed by
								 * a child transaction, even if the commit timestamp is higher than ours), nor
								 * readUncommitted (where we could see anything). Thus, we must see data ONLY if
								 * the txn is committed AND it was committed before we started.
								 */
								Long effectiveCommitTimestamp = met2.getEffectiveCommitTimestamp(met2.getParent());
								if(effectiveCommitTimestamp==null){
										/*
										 * It is possible that a transaction may have been committed, but whose
										 * parent has not yet committed, which would mean the global commit timestamp
										 * is not yet present. In that case, the effectiveCommitTimestamp is null,
										 * and so we must assume that the row is not committed
										 */
										visible = false;
								}else{
										long et1BeginTimestamp = myTransaction.getBeginTimestamp();
										visible = (effectiveCommitTimestamp < et1BeginTimestamp)
														|| myTransaction.isAncestorOf(otherTransaction);
								}
						}
				}
        return new VisibleResult(visible, otherTxnEffectiveStatus);
    }

    /**
     * @param t2
     * @param transactionSource allows the caller to plugin in a mechanism for loading transactions
     * @return indicator of whether and how this transaction writes would conflict with t2. This is based on the status,
     *         begin times, and lineage of the transactions.
     * @throws IOException
     */
    public ConflictType isInConflictWith(ImmutableTransaction t2, TransactionSource transactionSource) throws IOException {
        final ImmutableTransaction t1 = this;
        final ImmutableTransaction[] intersections = intersect(true, t1, t2);
        final ImmutableTransaction shared = intersections[0];
        final ImmutableTransaction et1 = intersections[1];
        final ImmutableTransaction et2 = intersections[2];
        final Transaction met2 = transactionSource.getTransaction(et2.getTransactionId().getId());
        if (met2.status.isCommitted()) {
            if (et1.isAncestorOf(et2)) {
                return ConflictType.CHILD;
            }
            return (met2.getEffectiveCommitTimestamp(shared) > et1.getBeginTimestamp()) ? ConflictType.SIBLING : ConflictType.NONE;
        } else if (met2.status.isActive()) {
            if (et1.sameTransaction(et2)
                    || et1.immutableParent.sameTransaction(et2)) {
                return ConflictType.NONE;
            } else if (et1.isAncestorOf(et2)) {
                return ConflictType.CHILD;
            } else {
                return ConflictType.SIBLING;
            }
        } else {
            return ConflictType.NONE;
        }
    }

    /**
     * Produce the chain of all transactions from this transaction to the root transaction.
     *
     * @throws IOException
     */
    private List<ImmutableTransaction> getChain() throws IOException {
        List<ImmutableTransaction> result = new ArrayList<ImmutableTransaction>();
        ImmutableTransaction current = this;
        do {
            result.add(current);
            current = current.immutableParent;
        } while (current != null);
        return Collections.unmodifiableList(result);
    }

    /**
     * The core function for determining the relationship between two transactions. This walks the tree of nested
     * transactions and returns three transactions in this order: the transactions which is the lowest point containing
     * both transactions (the intersection transaction),
     * the immediate containing transaction1 (the effective transaction1),
     * and the immediate child of this point containing transaction2 (the effective transaction2).
     * Note: if transaction1 or transaction2 is the same as the as the point of intersection then they will be used
     * as their respective effective transaction.
     *
     * @param collapse     if true then t2 is always used as effective transaction 2
     * @param transaction1
     * @param transaction2
     * @return three transactions as described above
     * @throws IOException
     */
    private ImmutableTransaction[] intersect(boolean collapse, ImmutableTransaction transaction1, ImmutableTransaction transaction2) throws IOException {
        final IntersectArgs key = new IntersectArgs(collapse, transaction1, transaction2);
        ImmutableTransaction[] result = intersectCache.get(key);
        if (result == null) {
            result = intersectDirect(collapse, transaction1, transaction2);
            intersectCache.put(key, result);
        }
        return result;
    }

    private static ImmutableTransaction[] intersectDirect(boolean collapse, ImmutableTransaction transaction1, ImmutableTransaction transaction2) throws IOException {
        List<ImmutableTransaction> chain1 = transaction1.getChain();
        List<ImmutableTransaction> chain2 = transaction2.getChain();
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

}
