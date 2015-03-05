package com.splicemachine.si.api;

import com.google.common.collect.Iterators;
import com.splicemachine.si.impl.ConflictType;
import com.splicemachine.utils.ByteSlice;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 * @author Scott Fines
 * Date: 6/18/14
 */
public interface Txn extends TxnView{
    static final Logger TXN_LOGGER = Logger.getLogger(Txn.class);

    static final Txn ROOT_TRANSACTION = new Txn() {
        @Override public String toString(){ return "ROOT"; }
        @Override public Iterator<ByteSlice> getDestinationTables() { return Iterators.emptyIterator();}
        @Override public boolean descendsFrom(TxnView potentialParent) { return false; }
        @Override public State getEffectiveState() { return State.ACTIVE; }
        @Override public IsolationLevel getIsolationLevel() { return IsolationLevel.SNAPSHOT_ISOLATION; }
        @Override public long getTxnId() { return -1l; }
        @Override public long getBeginTimestamp() { return 0; }
        @Override public long getCommitTimestamp() { return -1l; }
        @Override public long getEffectiveCommitTimestamp() { return -1l; }
        @Override public long getEffectiveBeginTimestamp() { return 0; }

        @Override public long getLastKeepAliveTimestamp() { return -1l; }

        @Override public TxnView getParentTxnView() { return null; }
        @Override public long getParentTxnId() { return -1l; }

        @Override public State getState() { return State.ACTIVE; }
        @Override public boolean allowsWrites() { return true; }
		@Override public String getSavePointName() { return null; }
		@Override public void setSavePointName(String savePointName) { }

        @Override
        public void commit() throws IOException {
            throw new UnsupportedOperationException("Cannot commit the root transaction");
        }

        @Override
        public void rollback() throws IOException {
            throw new UnsupportedOperationException("Cannot rollback the root transaction");
        }

        @Override
        public Txn elevateToWritable(byte[] writeTable) throws IOException {
            throw new UnsupportedOperationException("Cannot elevate the root transaction");
        }

        @Override public boolean canSee(TxnView otherTxn) { return false; }

        @Override public boolean isAdditive() { return false; }

        @Override public long getGlobalCommitTimestamp() { return -1l; }

        @Override
        public ConflictType conflicts(TxnView otherTxn) {
            return ConflictType.CHILD; //every transaction is a child of this
        }
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			throw new RuntimeException("Not Implemented");			
		}
		@Override
		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
			throw new RuntimeException("Not Implemented");			
		}
    };

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

        public static State fromInt(int b) {
            switch(b){
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
                    throw new IllegalArgumentException("Unknown transaction state! "+ b);

            }
        }

				public static State fromByte(byte b) {
						switch(b){
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
										throw new IllegalArgumentException("Unknown transaction state! "+ b);

						}
				}

        public static State fromString(String string) {
            if(ROLLEDBACK.name().equalsIgnoreCase(string)) return ROLLEDBACK;
            else if(COMMITTED.name().equalsIgnoreCase(string)) return COMMITTED;
            else if(ACTIVE.name().equalsIgnoreCase(string)) return ACTIVE;
            else
                throw new IllegalArgumentException("Cannot parse Transaction state from string "+ string);
        }
    }

		public enum IsolationLevel{
				READ_UNCOMMITTED(1){
						@Override
						public boolean canSee(long beginTimestamp, TxnView otherTxn,boolean isParent) {
								return otherTxn.getState()!=State.ROLLEDBACK;
						}

            @Override public String toHumanFriendlyString() { return "READ UNCOMMITTED"; }
        },
				READ_COMMITTED(2){
						@Override
						public boolean canSee(long beginTimestamp, TxnView otherTxn,boolean isParent) {
                return otherTxn.getState()==State.COMMITTED;
//								if(otherTxn.getState() !=State.COMMITTED) return false; //if itself hasn't been committed, it can't be seen
//								State effectiveState = otherTxn.getEffectiveState();
//								if(effectiveState==State.ROLLEDBACK) return false; //if it's been effectively rolled back, it can't be seen
//								//if we are a parent situation, then the effective state is active, but we can still see it.
//								return isParent || effectiveState == State.COMMITTED;
						}

            @Override public String toHumanFriendlyString() { return "READ COMMITTED"; }
        },
				SNAPSHOT_ISOLATION(3){
						@Override
						public boolean canSee(long beginTimestamp, TxnView otherTxn,boolean isParent) {
                return otherTxn.getState()==State.COMMITTED && otherTxn.getCommitTimestamp() <= beginTimestamp;
						}

            @Override public String toHumanFriendlyString() { return "SNAPSHOT ISOLATION"; }
        };
				protected final int level;

				IsolationLevel(int level) {
						this.level = level;
				}

				public boolean canSee(long beginTimestamp, TxnView otherTxn,boolean isParent) {
						throw new UnsupportedOperationException();
				}

				public int getLevel() { return level; }

        public static IsolationLevel fromInt(int val) {
            switch(val){
                case 1:
                    return READ_UNCOMMITTED;
                case 2:
                    return READ_COMMITTED;
                default:
                    return SNAPSHOT_ISOLATION;
            }
        }

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

				public byte encode() {
						return (byte)level;
				}

        public String toHumanFriendlyString() {
            throw new AbstractMethodError();
        }
    }

		public void setSavePointName(String savePointName);

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
		 * Elevate the transaction to a writable transaction, if it is not currently writable.
		 *
		 * @param writeTable the table to which this transaction intends to modify. This acts as a
		 *                   DDL lock on that table--no DDL operation can proceed(except the owner of this
		 *                   transaction or its parent) while this transaction remains active.
		 * @return a transaction set up such that {@link #allowsWrites()} returns {@code true}
		 * @throws IOException if something goes wrong during the elevation
		 */
		Txn elevateToWritable(byte[] writeTable) throws IOException;
}
