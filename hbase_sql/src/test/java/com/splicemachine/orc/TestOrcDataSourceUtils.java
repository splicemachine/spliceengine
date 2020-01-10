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
package com.splicemachine.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.junit.Test;
import java.util.List;
import static com.splicemachine.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static org.junit.Assert.assertEquals;

public class TestOrcDataSourceUtils
{
    @Test
    public void testMergeSingle()
    {
        List<DiskRange> diskRanges = mergeAdjacentDiskRanges(
                ImmutableList.of(new DiskRange(100, 100)),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE));
        assertEquals(diskRanges, ImmutableList.of(new DiskRange(100, 100)));
    }

    @Test
    public void testMergeAdjacent()
    {
        List<DiskRange> diskRanges = mergeAdjacentDiskRanges(
                ImmutableList.of(new DiskRange(100, 100), new DiskRange(200, 100), new DiskRange(300, 100)),
                new DataSize(0, BYTE),
                new DataSize(1, GIGABYTE));
        assertEquals(diskRanges, ImmutableList.of(new DiskRange(100, 300)));
    }

    @Test
    public void testMergeGap()
    {
        List<DiskRange> consistent10ByteGap = ImmutableList.of(new DiskRange(100, 90), new DiskRange(200, 90), new DiskRange(300, 90));
        assertEquals(mergeAdjacentDiskRanges(consistent10ByteGap, new DataSize(0, BYTE), new DataSize(1, GIGABYTE)), consistent10ByteGap);
        assertEquals(mergeAdjacentDiskRanges(consistent10ByteGap, new DataSize(9, BYTE), new DataSize(1, GIGABYTE)), consistent10ByteGap);
        assertEquals(mergeAdjacentDiskRanges(consistent10ByteGap, new DataSize(10, BYTE), new DataSize(1, GIGABYTE)), ImmutableList.of(new DiskRange(100, 290)));
        assertEquals(mergeAdjacentDiskRanges(consistent10ByteGap, new DataSize(100, BYTE), new DataSize(1, GIGABYTE)), ImmutableList.of(new DiskRange(100, 290)));

        List<DiskRange> middle10ByteGap = ImmutableList.of(new DiskRange(100, 80), new DiskRange(200, 90), new DiskRange(300, 80), new DiskRange(400, 90));
        assertEquals(mergeAdjacentDiskRanges(middle10ByteGap, new DataSize(0, BYTE), new DataSize(1, GIGABYTE)), middle10ByteGap);
        assertEquals(mergeAdjacentDiskRanges(middle10ByteGap, new DataSize(9, BYTE), new DataSize(1, GIGABYTE)), middle10ByteGap);
        assertEquals(mergeAdjacentDiskRanges(middle10ByteGap, new DataSize(10, BYTE), new DataSize(1, GIGABYTE)),
                ImmutableList.of(new DiskRange(100, 80), new DiskRange(200, 180), new DiskRange(400, 90)));
        assertEquals(mergeAdjacentDiskRanges(middle10ByteGap, new DataSize(100, BYTE), new DataSize(1, GIGABYTE)), ImmutableList.of(new DiskRange(100, 390)));
    }

    @Test
    public void testMergeMaxSize()
    {
        List<DiskRange> consistent10ByteGap = ImmutableList.of(new DiskRange(100, 90), new DiskRange(200, 90), new DiskRange(300, 90));
        assertEquals(mergeAdjacentDiskRanges(consistent10ByteGap, new DataSize(10, BYTE), new DataSize(0, BYTE)), consistent10ByteGap);
        assertEquals(mergeAdjacentDiskRanges(consistent10ByteGap, new DataSize(10, BYTE), new DataSize(100, BYTE)), consistent10ByteGap);
        assertEquals(mergeAdjacentDiskRanges(consistent10ByteGap, new DataSize(10, BYTE), new DataSize(190, BYTE)),
                ImmutableList.of(new DiskRange(100, 190), new DiskRange(300, 90)));
        assertEquals(mergeAdjacentDiskRanges(consistent10ByteGap, new DataSize(10, BYTE), new DataSize(200, BYTE)),
                ImmutableList.of(new DiskRange(100, 190), new DiskRange(300, 90)));
        assertEquals(mergeAdjacentDiskRanges(consistent10ByteGap, new DataSize(10, BYTE), new DataSize(290, BYTE)), ImmutableList.of(new DiskRange(100, 290)));

        List<DiskRange> middle10ByteGap = ImmutableList.of(new DiskRange(100, 80), new DiskRange(200, 90), new DiskRange(300, 80), new DiskRange(400, 90));
        assertEquals(mergeAdjacentDiskRanges(middle10ByteGap, new DataSize(0, BYTE), new DataSize(1, GIGABYTE)), middle10ByteGap);
        assertEquals(mergeAdjacentDiskRanges(middle10ByteGap, new DataSize(9, BYTE), new DataSize(1, GIGABYTE)), middle10ByteGap);
        assertEquals(mergeAdjacentDiskRanges(middle10ByteGap, new DataSize(10, BYTE), new DataSize(1, GIGABYTE)),
                ImmutableList.of(new DiskRange(100, 80), new DiskRange(200, 180), new DiskRange(400, 90)));
        assertEquals(mergeAdjacentDiskRanges(middle10ByteGap, new DataSize(100, BYTE), new DataSize(1, GIGABYTE)), ImmutableList.of(new DiskRange(100, 390)));
    }
}
