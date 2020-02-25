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
    private final boolean purge;

    public PurgeConfig(boolean purge, PurgeLatestTombstone purgeLatestTombstone, boolean respectActiveTransactions) {
        this.purge = purge;
        this.purgeLatestTombstone = purgeLatestTombstone;
        this.respectActiveTransactions = respectActiveTransactions;
    }

    public static PurgeConfig forcePurgeConfig() {
        return new PurgeConfig(true, PurgeLatestTombstone.ALWAYS, false);
    }

    public static PurgeConfig purgeDuringFlushConfig() {
        return new PurgeConfig(true, PurgeLatestTombstone.IF_FIRST_WRITE_PRESENT, true);
    }

    public static PurgeConfig purgeDuringMinorCompactionConfig() {
        return new PurgeConfig(true, PurgeLatestTombstone.IF_DELETE_FOLLOWS_FIRST_WRITE, true);
    }

    public static PurgeConfig purgeDuringMajorCompactionConfig() {
        return new PurgeConfig(true, PurgeLatestTombstone.ALWAYS, true);
    }

    public static PurgeConfig noPurgeConfig() {
        return new PurgeConfig(false, PurgeLatestTombstone.ALWAYS, true);
    }

    public PurgeLatestTombstone getPurgeLatestTombstone() {
        return purgeLatestTombstone;
    }

    public boolean shouldRespectActiveTransactions() {
        return respectActiveTransactions;
    }

    public boolean shouldPurge() {
        return purge;
    }
}

