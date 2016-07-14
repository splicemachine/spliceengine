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

package com.splicemachine.storage.util;

import org.sparkproject.guava.base.Predicate;
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
