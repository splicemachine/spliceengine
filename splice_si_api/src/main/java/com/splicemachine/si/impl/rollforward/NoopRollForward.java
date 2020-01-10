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

package com.splicemachine.si.impl.rollforward;

import com.splicemachine.si.api.rollforward.RollForward;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.ByteSlice;

import java.util.List;

/**
 * @author Scott Fines
 * Date: 7/1/14
 */
public class NoopRollForward implements RollForward {
    public static final RollForward INSTANCE = new NoopRollForward();

    private NoopRollForward() {
    }

    @Override
    public void submitForResolution(Partition partition, long txnId, List<ByteSlice> rowKey) {
    }

    @Override
    public int getFirstQueueSize() {
        return 0;
    }

    @Override
    public int getSecondQueueSize() {
        return 0;
    }

    @Override
    public long getFirstQueueResolutions() {
        return 0;
    }

    @Override
    public long getSecondQueueResolutions() {
        return 0;
    }

    @Override
    public long getFirstQueueActive() {
        return 0;
    }

    @Override
    public long getSecondQueueActive() {
        return 0;
    }
}
