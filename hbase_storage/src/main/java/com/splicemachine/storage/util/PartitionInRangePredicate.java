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

package com.splicemachine.storage.util;

import splice.com.google.common.base.Predicate;
import com.splicemachine.storage.Partition;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nullable;

/**
 * Simple Predicate to test whether a partition is in range.
 */
public class PartitionInRangePredicate implements Predicate<Partition> {
    private byte[] startKey;
    private byte[] endKey;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public PartitionInRangePredicate(byte[] startKey, byte[] endKey) {
            this.startKey = startKey;
            this.endKey = endKey;
    }
    @Override
    public boolean apply(@Nullable Partition partition) {
        assert partition!=null: "Partition cannot be null";
        return partition.overlapsRange(startKey,endKey);
    }
}
