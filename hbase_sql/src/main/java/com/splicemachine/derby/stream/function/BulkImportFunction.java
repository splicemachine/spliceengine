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

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;

/**
 * Created by dgomezferro on 5/17/17.
 */
public class BulkImportFunction implements VoidFunction<BulkImportPartition>, Externalizable {
    private static final Logger LOG = Logger.getLogger(BulkImportFunction.class);

    // serialization
    public BulkImportFunction() {}

    public BulkImportFunction(String bulkImportDirectory) {
        this.bulkImportDirectory = bulkImportDirectory;
    }

    String bulkImportDirectory;

    @Override
    public void call(BulkImportPartition importPartition) throws Exception {
        Configuration conf = HConfiguration.unwrapDelegate();
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        FileSystem fs = FileSystem.get(URI.create(bulkImportDirectory), conf);
        Long conglomerateId = importPartition.getConglomerateId();
        PartitionFactory tableFactory= SIDriver.driver().getTableFactory();
        try(Partition partition=tableFactory.getTable(Long.toString(conglomerateId))){
            Path path = new Path(importPartition.getFilePath()).getParent();
            if (fs.exists(path)) {
                loader.doBulkLoad(path,(HTable) ((ClientPartition)partition).unwrapDelegate());
                fs.delete(path, true);
            } else {
                LOG.warn("Path doesn't exist, nothing to load into this partition? " + path);
            }
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "Loaded file %s", path.toString());
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(bulkImportDirectory);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bulkImportDirectory = (String) in.readObject();
        SpliceSpark.setupSpliceStaticComponents();
    }
}
