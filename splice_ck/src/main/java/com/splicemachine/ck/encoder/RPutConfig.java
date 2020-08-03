/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.ck.encoder;

public class RPutConfig {

    private boolean tombstone = false;
    private boolean antiTombstone = false;
    private boolean firstWrite = false;
    private boolean deleteAfterFirstWrite = false;
    private Long foreignKeyCounter = null;
    private String userData = null;
    private Long txnId = null;
    private Long commitTS = null;

    public boolean hasTombstone() {
        return tombstone;
    }

    public void setTombstone() {
        this.tombstone = true;
    }

    public boolean hasAntiTombstone() {
        return antiTombstone;
    }

    public void setAntiTombstone() {
        this.antiTombstone = true;
    }

    public boolean hasFirstWrite() {
        return firstWrite;
    }

    public void setFirstWrite() {
        this.firstWrite = true;
    }

    public boolean hasDeleteAfterFirstWrite() {
        return deleteAfterFirstWrite;
    }

    public void setDeleteAfterFirstWrite() {
        this.deleteAfterFirstWrite = true;
    }

    public boolean hasForeignKeyCounter() {
        return foreignKeyCounter != null;
    }

    public long getForeignKeyCounter() {
        assert foreignKeyCounter != null;
        return foreignKeyCounter;
    }

    public void setForeignKeyCounter(long foreignKeyCounter) {
        this.foreignKeyCounter = foreignKeyCounter;
    }

    public boolean hasUserData() {
        return userData != null;
    }

    public String getUserData() {
        return userData;
    }

    public void setUserData(String userData) {
        this.userData = userData;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public long getTxnId() {
        return txnId;
    }

    public void setCommitTS(long commitTS) {
        this.commitTS = commitTS;
    }

    public boolean hasCommitTS() {
        return commitTS == null;
    }

    public long getCommitTS() {
        return commitTS;
    }
}
