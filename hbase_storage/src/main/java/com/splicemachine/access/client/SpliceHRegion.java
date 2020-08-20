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

import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by jyuan on 5/13/19.
 */
public class SpliceHRegion extends HRegion {
    private static final Logger LOG = Logger.getLogger(SpliceHRegion.class);

    public SpliceHRegion(final Path tableDir, final WAL wal, final FileSystem fs,
                         final Configuration confParam, final RegionInfo regionInfo,
                         final TableDescriptor htd, final RegionServerServices rsServices,
                         Set<String> compactedFilesPaths) throws IOException{

        super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
        openHRegion(null);
        setCompactedFiles(compactedFilesPaths);
    }

    @Override
    public CellComparator getCellComparator() {
        return SpliceCellComparator.INSTANCE;
    }

    private void setCompactedFiles(Set<String> compactedFilesPaths) {
        try {
            List<HStoreFile> compactedFiles = new ArrayList<>();
            HStore store = this.getStore(SIConstants.DEFAULT_FAMILY_BYTES);

            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "regionName " + this.getRegionInfo().getRegionNameAsString());
                SpliceLogUtils.debug(LOG, "compactedFilePaths to add: " + compactedFilesPaths);
                SpliceLogUtils.debug(LOG, "region store files: " + store.getStorefiles());
            }
            for (HStoreFile storeFile: store.getStorefiles()) {
                if (compactedFilesPaths.contains(storeFile.getPath().toString())) {
                    compactedFiles.add(storeFile);
                }
            }
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "filtered compacted files: " + compactedFiles);
            }
            HRegionUtil.replaceStoreFiles(store, compactedFiles, ImmutableList.of());

        } catch (Throwable e) {
            SpliceLogUtils.error(LOG, "Unable to set Compacted Files from HBase region server", e);
            throw new RuntimeException(e);
        }
    }
}
