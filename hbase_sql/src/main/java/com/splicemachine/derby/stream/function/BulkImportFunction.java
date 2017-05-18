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
 *
 */

package com.splicemachine.derby.stream.function;

import com.google.common.collect.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by dgomezferro on 5/17/17.
 */
public class BulkImportFunction implements FlatMapFunction<BulkImportPartition, StandardException>, Serializable {
    private static final Logger LOG = Logger.getLogger(BulkImportFunction.class);

    public BulkImportFunction(String bulkImportDirectory) {
        this.bulkImportDirectory = bulkImportDirectory;
    }

    String bulkImportDirectory;

    @Override
    public Iterator<StandardException> call(BulkImportPartition importPartition) {
        try {
            Configuration conf = HConfiguration.unwrapDelegate();
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            FileSystem fs = FileSystem.get(URI.create(bulkImportDirectory), conf);
            Long conglomerateId = importPartition.getConglomerateId();
            PartitionFactory tableFactory= SIDriver.driver().getTableFactory();
            try(Partition partition=tableFactory.getTable(Long.toString(conglomerateId))){
                Path path = new Path(importPartition.getFilePath()).getParent();
                if (fs.exists(path)) {
                    loader.doBulkLoad(path,(HTable) ((ClientPartition)partition).unwrapDelegate());
                } else {
                    LOG.warn("Path doesn't exist, nothing to load into this partition? " + path);
                }
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "Loaded file %s", path.toString());
                }
            }
        } catch (Exception e) {
            return Arrays.asList(StandardException.plainWrapException(e)).iterator();
        }
        return Collections.emptyIterator();
    }
}
