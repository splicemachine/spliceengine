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

package com.splicemachine.si.impl.server;

public class PurgeConfig {
    public enum PurgeLatestTombstone {
        ALWAYS,
        IF_DELETE_FOLLOWS_FIRST_WRITE,
        IF_FIRST_WRITE_PRESENT;
    }

    private final PurgeLatestTombstone purgeLatestTombstone;
    private final boolean respectActiveTransactions;
    private final boolean purgeDeletes;
    private final boolean purgeUpdates;
    private final long transactionLowWatermark;

    public PurgeConfig(boolean purgeDeletes, PurgeLatestTombstone purgeLatestTombstone,
                       boolean purgeUpdates, boolean respectActiveTransactions, long transactionLowWatermark) {
        this.purgeDeletes = purgeDeletes;
        this.purgeLatestTombstone = purgeLatestTombstone;
        this.purgeUpdates = purgeUpdates;
        this.respectActiveTransactions = respectActiveTransactions;
        this.transactionLowWatermark = transactionLowWatermark;
    }

    public PurgeLatestTombstone getPurgeLatestTombstone() {
        return purgeLatestTombstone;
    }

    public boolean shouldRespectActiveTransactions() {
        return respectActiveTransactions;
    }

    public boolean shouldPurgeDeletes() {
        return purgeDeletes;
    }

    public boolean shouldPurgeUpdates() {
        return purgeUpdates;
    }

    public long getTransactionLowWatermark() {
        return transactionLowWatermark;
    }
}

