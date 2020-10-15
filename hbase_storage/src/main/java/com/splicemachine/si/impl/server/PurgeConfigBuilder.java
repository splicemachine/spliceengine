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

import java.io.IOException;

public class PurgeConfigBuilder {
    private PurgeConfig.PurgeLatestTombstone purgeLatestTombstone = null;
    private Boolean respectActiveTransactions = null;
    private Boolean purgeDeletes = null;
    private Boolean purgeUpdates = null;
    private Long transactionLowWatermark = null;

    public PurgeConfigBuilder purgeDeletes(boolean b) {
        assert purgeDeletes == null;
        purgeDeletes = b;
        return this;
    }

    public PurgeConfigBuilder purgeUpdates(boolean b) {
        assert purgeUpdates == null;
        purgeUpdates = b;
        return this;
    }

    public PurgeConfigBuilder respectActiveTransactions(boolean b) {
        assert respectActiveTransactions == null;
        respectActiveTransactions = b;
        return this;
    }

    public PurgeConfigBuilder purgeLatestTombstone(PurgeConfig.PurgeLatestTombstone value) {
        assert purgeLatestTombstone == null;
        purgeLatestTombstone = value;
        return this;
    }

    public PurgeConfigBuilder forcePurgeDeletes() {
        purgeDeletes(true);
        respectActiveTransactions(false);
        purgeLatestTombstone(PurgeConfig.PurgeLatestTombstone.ALWAYS);
        return this;
    }

    public PurgeConfigBuilder purgeDeletesDuringFlush() {
        purgeDeletes(true);
        respectActiveTransactions(true);
        purgeLatestTombstone(PurgeConfig.PurgeLatestTombstone.IF_FIRST_WRITE_PRESENT);
        return this;
    }

    public PurgeConfigBuilder purgeDeletesDuringMinorCompaction() {
        purgeDeletes(true);
        respectActiveTransactions(true);
        purgeLatestTombstone(PurgeConfig.PurgeLatestTombstone.IF_DELETE_FOLLOWS_FIRST_WRITE);
        return this;
    }

    public PurgeConfigBuilder purgeDeletesDuringMajorCompaction() {
        purgeDeletes(true);
        respectActiveTransactions(true);
        purgeLatestTombstone(PurgeConfig.PurgeLatestTombstone.ALWAYS);
        return this;
    }

    public PurgeConfigBuilder purgeDeletesDuringCompaction(boolean major) {
        if (major)
            purgeDeletesDuringMajorCompaction();
        else
            purgeDeletesDuringMinorCompaction();
        return this;
    }

    public PurgeConfigBuilder noPurgeDeletes() {
        purgeDeletes(false);
        respectActiveTransactions(true);
        purgeLatestTombstone(PurgeConfig.PurgeLatestTombstone.ALWAYS);
        return this;
    }

    public PurgeConfigBuilder noPurge() {
        noPurgeDeletes();
        purgeUpdates(false);
        return this;
    }

    public PurgeConfigBuilder transactionLowWatermark(long value) {
        assert transactionLowWatermark == null;
        transactionLowWatermark = value;
        return this;
    }

    public PurgeConfig build() throws IOException {
        assert purgeDeletes != null;
        assert purgeUpdates != null;
        assert purgeLatestTombstone != null;
        assert respectActiveTransactions != null;
        assert transactionLowWatermark != null;
        return new PurgeConfig(purgeDeletes, purgeLatestTombstone, purgeUpdates, respectActiveTransactions, transactionLowWatermark);
    }
}

