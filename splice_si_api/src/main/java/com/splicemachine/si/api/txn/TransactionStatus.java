/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.api.txn;

/**
 * An SI transaction passes through the following states.
 * The successful path is ACTIVE -> COMMITTING -> COMMITTED.
 * Alternate paths are ACTIVE -> ERROR and ACTIVE -> ROLLED_BACK.
 */
public enum TransactionStatus {
    ACTIVE((byte) 0x00),
    ERROR((byte) 0x01),
    COMMITTING((byte) 0x02),
    COMMITTED((byte) 0x03),
    ROLLED_BACK((byte) 0x04);

    private final byte id;

    TransactionStatus(byte id) {
        this.id = id;
    }

    private boolean isActive() {
        // COMITTING is considered ACTIVE, since we still don't know what will happen with the transaction
        return this.equals(ACTIVE) || this.equals(COMMITTING);
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

    public static TransactionStatus forByte(byte b) {
        switch (b) {
            case 0:
                return ACTIVE;
            case 1:
                return ERROR;
            case 2:
                return COMMITTING;
            case 3:
                return COMMITTED;
            case 4:
                return ROLLED_BACK;
            default:
                throw new IllegalArgumentException("Unknown transaction id!");
        }
    }

    /*
     * Kept for backwards compatibility (for transaction tables
     * which are encoded using an int).
     */
    public static TransactionStatus forInt(int value) {
        switch (value) {
            case 0:
                return ACTIVE;
            case 1:
                return ERROR;
            case 2:
                return COMMITTING;
            case 3:
                return COMMITTED;
            case 4:
                return ROLLED_BACK;
            default:
                throw new IllegalArgumentException("Unknown transaction id!");
        }
    }
}
