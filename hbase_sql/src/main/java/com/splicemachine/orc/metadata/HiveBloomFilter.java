/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
package com.splicemachine.orc.metadata;

import com.google.common.primitives.Longs;
import org.apache.hive.common.util.BloomFilter;

import java.util.List;

public class HiveBloomFilter extends BloomFilter
{
    // constructor that allows deserialization of a long list into the actual hive bloom filter
    public HiveBloomFilter(List<Long> bits, int numBits, int numHashFunctions)
    {
        this.bitSet = new BitSet(Longs.toArray(bits));
        this.numBits = numBits;
        this.numHashFunctions = numHashFunctions;
    }

    public HiveBloomFilter(BloomFilter bloomFilter)
    {
        this.bitSet = new BitSet(bloomFilter.getBitSet().clone());
        this.numBits = bloomFilter.getBitSize();
        this.numHashFunctions = bloomFilter.getNumHashFunctions();
    }
}
