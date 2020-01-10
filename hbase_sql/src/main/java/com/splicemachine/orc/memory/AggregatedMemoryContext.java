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
package com.splicemachine.orc.memory;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AggregatedMemoryContext
        extends AbstractAggregatedMemoryContext
{
    // This class should remain exactly the same as AggregatedMemoryContext in com.splicemachine.memory

    private final AbstractAggregatedMemoryContext parentMemoryContext;
    private long usedBytes;
    private boolean closed;

    public AggregatedMemoryContext()
    {
        this.parentMemoryContext = null;
    }

    public AggregatedMemoryContext(AbstractAggregatedMemoryContext parentMemoryContext)
    {
        this.parentMemoryContext = requireNonNull(parentMemoryContext, "parentMemoryContext is null");
    }

    public long getBytes()
    {
        checkState(!closed);
        return usedBytes;
    }

    @Override
    protected void updateBytes(long bytes)
    {
        checkState(!closed);
        if (parentMemoryContext != null) {
            parentMemoryContext.updateBytes(bytes);
        }
        usedBytes += bytes;
    }

    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        if (parentMemoryContext != null) {
            parentMemoryContext.updateBytes(-usedBytes);
        }
        usedBytes = 0;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("usedBytes", usedBytes)
                .add("closed", closed)
                .toString();
    }
}
