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

package com.splicemachine.access.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.wal.WAL;

import java.io.IOException;

/**
 * Created by jyuan on 5/13/19.
 */
public class SpliceHRegion extends HRegion {


    public SpliceHRegion(final Path tableDir, final WAL wal, final FileSystem fs,
                         final Configuration confParam, final RegionInfo regionInfo,
                         final TableDescriptor htd, final RegionServerServices rsServices) throws IOException{

        super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
        openHRegion(null);
    }

    @Override
    public CellComparator getCellComparator() {
        return SpliceCellComparator.INSTANCE;
    }

}
