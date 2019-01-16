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
package com.splicemachine.orc;

import io.airlift.stats.DistributionStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class FileFormatDataSourceStats
{
    private final DistributionStat readBytes = new DistributionStat();
    private final DistributionStat maxCombinedBytesPerRow = new DistributionStat();
    private final TimeStat time0Bto100KB = new TimeStat(MILLISECONDS);
    private final TimeStat time100KBto1MB = new TimeStat(MILLISECONDS);
    private final TimeStat time1MBto10MB = new TimeStat(MILLISECONDS);
    private final TimeStat time10MBPlus = new TimeStat(MILLISECONDS);

    @Managed
    @Nested
    public DistributionStat getReadBytes()
    {
        return readBytes;
    }

    @Managed
    @Nested
    public DistributionStat getMaxCombinedBytesPerRow()
    {
        return maxCombinedBytesPerRow;
    }

    @Managed
    @Nested
    public TimeStat get0Bto100KB()
    {
        return time0Bto100KB;
    }

    @Managed
    @Nested
    public TimeStat get100KBto1MB()
    {
        return time100KBto1MB;
    }

    @Managed
    @Nested
    public TimeStat get1MBto10MB()
    {
        return time1MBto10MB;
    }

    @Managed
    @Nested
    public TimeStat get10MBPlus()
    {
        return time10MBPlus;
    }

    public void readDataBytesPerSecond(long bytes, long nanos)
    {
        readBytes.add(bytes);
        if (bytes < 100 * 1024) {
            time0Bto100KB.add(nanos, NANOSECONDS);
        }
        else if (bytes < 1024 * 1024) {
            time100KBto1MB.add(nanos, NANOSECONDS);
        }
        else if (bytes < 10 * 1024 * 1024) {
            time1MBto10MB.add(nanos, NANOSECONDS);
        }
        else {
            time10MBPlus.add(nanos, NANOSECONDS);
        }
    }

    public void addMaxCombinedBytesPerRow(long bytes)
    {
        maxCombinedBytesPerRow.add(bytes);
    }
}

