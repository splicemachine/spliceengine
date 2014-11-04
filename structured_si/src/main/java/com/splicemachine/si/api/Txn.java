package com.splicemachine.si.api;

import com.splicemachine.si.impl.RootTransaction;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 6/18/14
 */
public interface Txn extends TxnView{
		static final Logger TXN_LOGGER = Logger.getLogger(Txn.class);

    static final Txn ROOT_TRANSACTION = new RootTransaction(); 

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
