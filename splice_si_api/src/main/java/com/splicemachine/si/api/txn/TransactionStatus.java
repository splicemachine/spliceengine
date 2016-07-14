/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
