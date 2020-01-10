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

public abstract class AbstractAggregatedMemoryContext
{
    // This class should remain exactly the same as AbstractAggregatedMemoryContext in com.splicemachine.memory

    // AbstractMemoryContext class is only necessary because we need implementations that bridge
    // AggregatedMemoryContext with the existing memory tracking APIs in XxxxContext. Once they
    // are refactored, there will be only one implementation of this abstract class, and this class
    // can be removed.

    protected abstract void updateBytes(long bytes);

    public AggregatedMemoryContext newAggregatedMemoryContext()
    {
        return new AggregatedMemoryContext(this);
    }

    public LocalMemoryContext newLocalMemoryContext()
    {
        return new LocalMemoryContext(this);
    }
}
