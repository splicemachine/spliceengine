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

package com.splicemachine.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.ExploringCompactionPolicy;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Simple compaction policy extending HBase's default in order to create our own CompactionRequest
 */
public class SpliceDefaultCompactionPolicy extends ExploringCompactionPolicy {
    /**
     * Constructor for ExploringCompactionPolicy.
     *
     * @param conf            The configuration object
     * @param storeConfigInfo An object to provide info about the store.
     */
    public SpliceDefaultCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
        super(conf, storeConfigInfo);
    }

    @Override
    public CompactionRequest selectCompaction(Collection<StoreFile> candidateFiles, List<StoreFile> filesCompacting, boolean isUserCompaction, boolean mayUseOffPeak, boolean forceMajor) throws IOException {
        CompactionRequest cr = super.selectCompaction(candidateFiles, filesCompacting, isUserCompaction, mayUseOffPeak, forceMajor);
        SpliceCompactionRequest scr = new SpliceCompactionRequest();
        scr.combineWith(cr);
        return scr;
    }
}
