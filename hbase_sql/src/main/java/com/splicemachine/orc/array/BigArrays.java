/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.orc.array;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/)
// Copyright (C) 2010-2013 Sebastiano Vigna
final class BigArrays
{
    private BigArrays() {}

    /**
     * Initial number of segments to support in array.
     */
    public static final int INITIAL_SEGMENTS = 1024;

    /**
     * The shift used to compute the segment associated with an index (equivalently, the logarithm of the segment size).
     */
    public static final int SEGMENT_SHIFT = 10;

    /**
     * Size of a single segment of a BigArray
     */
    public static final int SEGMENT_SIZE = 1 << SEGMENT_SHIFT;

    /**
     * The mask used to compute the offset associated to an index.
     */
    public static final int SEGMENT_MASK = SEGMENT_SIZE - 1;

    /**
     * Computes the segment associated with a given index.
     *
     * @param index an index into a big array.
     * @return the associated segment.
     */
    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static int segment(long index)
    {
        return (int) (index >>> SEGMENT_SHIFT);
    }

    /**
     * Computes the offset associated with a given index.
     *
     * @param index an index into a big array.
     * @return the associated offset (in the associated {@linkplain #segment(long) segment}).
     */
    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static int offset(long index)
    {
        return (int) (index & SEGMENT_MASK);
    }
}

