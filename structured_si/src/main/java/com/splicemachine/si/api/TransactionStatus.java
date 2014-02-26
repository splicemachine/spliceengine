package com.splicemachine.si.api;

/**
 * An SI transaction passes through the following states.
 * The successful path is ACTIVE -> COMMITTING -> COMMITTED.
 * Alternate paths are ACTIVE -> ERROR and ACTIVE -> ROLLED_BACK.
 */
public enum TransactionStatus {
    ACTIVE((byte)0x00),
    ERROR((byte)0x01),
    COMMITTING((byte)0x02),
    COMMITTED((byte)0x03),
    ROLLED_BACK((byte)0x04);

		private final byte id;

		TransactionStatus(byte id) {
				this.id = id;
		}

		public boolean isActive() {
        return this.equals(ACTIVE);
    }

    public boolean isFinished() {
        return !this.isActive();
    }

    public boolean isCommitted() {
        return this.equals(COMMITTED);
    }

    public boolean isCommitting() {
        return this.equals(COMMITTING);
    }

		public byte getId() {
				return id;
		}

		public static TransactionStatus forByte(byte b){
				switch (b){
						case 0: return ACTIVE;
						case 1: return ERROR;
						case 2: return COMMITTING;
						case 3: return COMMITTED;
						case 4: return ROLLED_BACK;
						default:
								throw new IllegalArgumentException("Unknown transaction id!");
				}
		}

		/*
		 * Kept for backwards compatibility (for transaction tables
		 * which are encoded using an int).
		 */
		public static TransactionStatus forInt(int value){
				switch (value){
						case 0: return ACTIVE;
						case 1: return ERROR;
						case 2: return COMMITTING;
						case 3: return COMMITTED;
						case 4: return ROLLED_BACK;
						default:
								throw new IllegalArgumentException("Unknown transaction id!");
				}
		}
}
