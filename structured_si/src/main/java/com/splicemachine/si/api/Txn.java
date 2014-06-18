package com.splicemachine.si.api;

import com.splicemachine.si.impl.ConflictType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * @author Scott Fines
 * Date: 6/18/14
 */
public interface Txn {
		static final Logger TXN_LOGGER = Logger.getLogger(Txn.class);

		static final Txn ROOT_TRANSACTION = new AbstractTxn(-1l,0,IsolationLevel.SNAPSHOT_ISOLATION) {
				@Override public boolean isDependent() { return false; }
				@Override public boolean isAdditive() { return false; }
				@Override public long getCommitTimestamp() { return -1l; }
				@Override public long getEffectiveCommitTimestamp() { return -1l; }
				@Override public Txn getParentTransaction() { return null; }
				@Override public State getState() { return State.ACTIVE; }
				@Override public boolean allowsWrites() { return true; }
				@Override public long getGlobalCommitTimestamp() { return -1l; }

				@Override
				public void commit() throws IOException {
					throw new UnsupportedOperationException("Root Transaction cannot be committed");
				}

				@Override
				public void rollback() throws IOException {
						throw new UnsupportedOperationException("Root Transaction cannot be rolled back");
				}

				@Override
				public void timeout() throws IOException {
						throw new UnsupportedOperationException("Root Transaction cannot timeout");
				}

				@Override
				public Txn elevateToWritable(byte[] writeTable) throws IOException {
						throw new UnsupportedOperationException("Root Transaction cannot elevate");
				}
		};

		Collection<byte[]> getDestinationTables();

		/**
		 * Determine if this transaction is a descendent of the specified transaction.
		 *
		 * @param potentialParent the transaction which may be the parent.
		 * @return true if this transaction is a descendent of the specified transaction.
		 * @throws  java.lang.NullPointerException if {@code potentialParent==null}.
		 */
		boolean descendsFrom(Txn potentialParent);


		public enum State{
				ACTIVE((byte)0x00), //represents an Active transaction that has not timed out
				COMMITTED((byte)0x03), //represents a committed transaction
				ROLLEDBACK((byte)0x04); //represents a rolled back transaction

				private byte id;
				private final byte[] idBytes;

				State(byte id) {
						this.id = id;
						this.idBytes = new byte[]{id};
				}

				public byte getId() { return id; }

				public byte[] encode(){
					return idBytes;
				}

				public static State decode(byte[] bytes, int offset,int length){
						if(length==4){
								int val = Bytes.toInt(bytes,offset,length);
								switch(val){
										case 0:
												return ACTIVE;
										case 2:
												TXN_LOGGER.warn("Seen the vestigial COMMITTING transaction state. " +
																"Treating it as ROLLED BACK, " +
																"since COMMITTING transactions are either stuck (e.g. timedout) or will be soon");
										case 1:
										case 4:
												return ROLLEDBACK;
										case 3:
												return COMMITTED;
										default:
												throw new IllegalArgumentException("Unknown transaction state! "+ val);

								}
						}else{
								byte b = bytes[offset];
								switch(b){
										case 0x00:
												return ACTIVE;
										case 0x02:
												TXN_LOGGER.warn("Seen the vestigial COMMITTING transaction state. " +
																"Treating it as ROLLED BACK, " +
																"since COMMITTING transactions are either stuck (e.g. timedout) or will be soon");
										case 0x01:
										case 0x04:
												return ROLLEDBACK;
										case 0x03:
												return COMMITTED;
										default:
												throw new IllegalArgumentException("Unknown transaction state! "+ b);
								}
						}

				}

				public boolean isFinal() {
						return this != ACTIVE;
				}
		}

		public enum IsolationLevel{
				READ_UNCOMMITTED(1){
						@Override
						public boolean canSee(long beginTimestamp, Txn otherTxn,boolean isParent) {
								return otherTxn.getEffectiveState()!=State.ROLLEDBACK;
						}
				},
				READ_COMMITTED(2){
						@Override
						public boolean canSee(long beginTimestamp, Txn otherTxn,boolean isParent) {
								if(otherTxn.getState() !=State.COMMITTED) return false; //if itself hasn't been committed, it can't be seen
								State effectiveState = otherTxn.getEffectiveState();
								if(effectiveState==State.ROLLEDBACK) return false; //if it's been effectively rolled back, it can't be seen
								//if we are a parent situation, then the effective state is active, but we can still see it.
								return isParent || effectiveState == State.COMMITTED;
						}
				},
				SNAPSHOT_ISOLATION(3){
						@Override
						public boolean canSee(long beginTimestamp, Txn otherTxn,boolean isParent) {
								return otherTxn.getEffectiveState() == State.COMMITTED
												&& otherTxn.getEffectiveCommitTimestamp() < beginTimestamp;
						}
				};
				private final int level;

				IsolationLevel(int level) {
						this.level = level;
				}

				public boolean canSee(long beginTimestamp, Txn otherTxn,boolean isParent) {
						throw new UnsupportedOperationException();
				}

				public int getLevel() { return level; }

				public static IsolationLevel fromByte(byte b) {
						switch(b){
								case 1:
										return READ_UNCOMMITTED;
								case 2:
										return READ_COMMITTED;
								default:
										return SNAPSHOT_ISOLATION;
						}
				}
		}

		public State getEffectiveState();

		/**
		 * @return the read isolation level for this transaction.
		 */
		IsolationLevel getIsolationLevel();

		/**
		 * @return the unique transaction id for this transaction.
		 */
		long getTxnId();

		/**
		 * @return the begin timestmap for the transaction.
		 */
		long getBeginTimestamp();

		/**
		 * Return the actual commit timestamp. If the transaction is dependent upon a parent, then this will
		 * return a different timestamp than that of {@link #getEffectiveCommitTimestamp()}.
		 *
		 * @return the actual commit timestamp for the transaction.
		 */
		long getCommitTimestamp();

		/**
		 * Return the "Effective" Commit timestamp.
		 *
		 * If the transaction is dependent upon a parent, then this will return the commit timestamp
		 * of the parent transaction. Otherwise, it will return the commit timestamp for the transaction itself.
		 *
		 * If the transaction has not been committed, then this will return -1;
		 *
		 * @return the effective commit timestamp, or -1 if the transaction should not be considered committed.
		 */
		long getEffectiveCommitTimestamp();

		/**
		 * Return the "Effective" Begin timestamp.
		 *
		 * This is necessary to deal with parent-child relationships.  To see why, consider the following case
		 *
		 * 1. Create parent with begin timestamp 1
		 * 2. Create independent tx with begin timestamp 2
		 * 3. write some data with txn 2
		 * 4. commit txn 2 with commit timestamp 3.
		 * 3. Create child of parent with begin timestamp 4.
		 *
		 * When that child reads data, his begin timestamp will occur after txn2's commit timestamp, implying
		 * that the result is visible. However, the child is a child of the parent, and that result is not visible
		 * to the parent (assuming the parent is in Snapshot Isolation Isolation level), so it shouldn't be visible
		 * to the child either.
		 *
		 * This is resolved by making the child return the begin timestamp of the parent as it's "effective" begin
		 * timestamp--the timestamp to be used when making these kinds of comparisons.
		 *
		 * @return the effective begin timestamp of the transaction. If the transaction has no parents, then its
		 * effective begin timestamp is the same as it's begin timestamp.
		 */
		long getEffectiveBeginTimestamp();

		/**
		 * @return the parent transaction for this transaction, or {@code null} if the transaction is a top-level
		 * txn.
		 */
		Txn getParentTransaction();

		/**
		 * @return the current state of the transaction
		 */
		State getState();

		/**
		 * Commit the transaction.
		 *
		 * @throws com.splicemachine.si.api.CannotCommitException if the transaction has already been rolled back
		 * @throws java.io.IOException if something goes wrong when attempting to commit
		 */
		void commit() throws IOException;

		/**
		 * Roll back the transaction.
		 *
		 * If the transaction has already been committed, this operation will do nothing.
		 *
		 * If the transaction has already been rolled back (or timed out), this operation will do nothing.
		 *
		 * @throws IOException If something goes wrong when attempting to rollback
		 */
		void rollback() throws IOException;

		/**
		 * Time out the transaction. A Timedout transaction behaves exactly as if it has been rolled back.
		 *
		 * If the transaction has already been committed, this operation will do nothing.
		 *
		 * If the transaction has already been rolled back (or timed out), this operation will do nothing.
		 * @throws IOException if something goes wrong when attempting to timeout
		 */
		void timeout() throws IOException;


		/**
		 * @return true if this transaction allows writes
		 */
		boolean allowsWrites();

		/**
		 * Elevate the transaction to a writable transaction, if it is not currently writable.
		 *
		 * @param writeTable the table to which this transaction intends to modify. This acts as a
		 *                   DDL lock on that table--no DDL operation can proceed(except the owner of this
		 *                   transaction or its parent) while this transaction remains active.
		 * @return a transaction set up such that {@link #allowsWrites()} returns {@code true}
		 * @throws IOException if something goes wrong during the elevation
		 */
		Txn elevateToWritable(byte[] writeTable) throws IOException;

		/**
		 * Determine if the other transaction is visible to this transaction (e.g. this instance can "see" writes
		 * made by {@code otherTxn}).
		 *
		 * The exact semantics of this method depends on the isolation level of the transaction:
		 *
		 * <ul>
		 *     <li>{@link com.splicemachine.si.api.Txn.IsolationLevel#READ_COMMITTED}:
		 *     writes are visible if {@code otherTxn} has not been rolled back</li>
		 *     <li>{@link com.splicemachine.si.api.Txn.IsolationLevel#READ_COMMITTED}:
		 *     writes are visible if {@code otherTxn} has been committed</li>
		 *     <li>{@link com.splicemachine.si.api.Txn.IsolationLevel#SNAPSHOT_ISOLATION}:
		 *     writes are visible if {@code otherTxn} has been committed <em>and</em>
		 *     {@code otherTxn.getEffectiveCommitTimestamp() < this.getBeginTimestamp()}
		 *     </li>
		 * </ul>
		 *
		 * Additionally, this method has distinct semantics which differ slightly:
		 * <ul>
		 *     <li>{@code this.equals(otherTxn)}: Writes are visible</li>
		 *     <li>{@code otherTxn} is a parent of {@code this}: Writes are visible</li>
		 * </ul>
		 *
		 * In all other circumstances, IsolationLevel semantics win.
		 *
		 * @param otherTxn the transaction to check
		 * @return true if this transaction can see writes made by {@code otherTxn}
		 */
		boolean canSee(Txn otherTxn);

		boolean isDependent();

		boolean isAdditive();

		long getGlobalCommitTimestamp();

		ConflictType conflicts(Txn otherTxn);
}
