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
 *
 */

package com.splicemachine.derby.stream.function;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.SkeletonHBaseClientPartition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.HBasePlatformUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by dgomezferro on 5/17/17.
 */
public class BulkImportFunction implements VoidFunction<Iterator<BulkImportPartition>>, Externalizable {
    private static final Logger LOG = Logger.getLogger(BulkImportFunction.class);
    private Map<Long, List<BulkImportPartition>> partitionMap;
    private byte[] token;

    // serialization
    public BulkImportFunction() {}

    public BulkImportFunction(String bulkImportDirectory, byte[] token) {
        this.bulkImportDirectory = bulkImportDirectory;
        this.token = token;
    }

    String bulkImportDirectory;

    @Override
    public void call(Iterator<BulkImportPartition> importPartitions) throws Exception {

        init(importPartitions);
        Configuration conf = HConfiguration.unwrapDelegate();
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        FileSystem fs = FileSystem.get(URI.create(bulkImportDirectory), conf);
        PartitionFactory tableFactory= SIDriver.driver().getTableFactory();

        for (Long conglomId : partitionMap.keySet()) {
            Partition partition=tableFactory.getTable(Long.toString(conglomId));
            List<BulkImportPartition> partitionList = partitionMap.get(conglomId);
            // For each batch of BulkImportPartition, use the first partition as staging area
            Path path = new Path(partitionList.get(0).getFilePath());
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }

            // Move files from all partitions to the first partition
            for (int i = 1; i < partitionList.size(); ++i) {
                Path sourceDir = new Path(partitionList.get(i).getFilePath());
                if (fs.exists(sourceDir)) {
                    FileStatus[] statuses = fs.listStatus(sourceDir);
                    for (FileStatus status : statuses) {
                        Path filePath = status.getPath();
                        Path destPath = new Path(path, filePath.getName());
                        fs.rename(filePath, destPath);
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG, "Move file %s to %s", filePath.toString(), destPath.toString());
                        }
                    }
                    fs.delete(sourceDir.getParent(), true);
                }
            }
            writeToken(fs, path);
            HBasePlatformUtils.bulkLoad(conf, loader, path.getParent(), "splice:" + partition.getTableName());
            fs.delete(path.getParent(), true);
        }
    }

    private void writeToken(FileSystem fs, Path path) throws IOException{
        if (token != null && token.length > 0) {
            FSDataOutputStream out = null;
            try {
                out = fs.create(new Path(path, "_token"));
                out.writeInt(token.length);
                out.write(token);
                out.close();
            }finally {
                if (out != null) {
                    out.close();
                }
            }
        }
    }
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(bulkImportDirectory);
        ArrayUtil.writeByteArray(out, token);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bulkImportDirectory = (String) in.readObject();
        token = ArrayUtil.readByteArray(in);
        SpliceSpark.setupSpliceStaticComponents();
    }

    private void init(Iterator<BulkImportPartition> importPartitions) throws Exception {
        partitionMap = new HashMap<>();
        while (importPartitions.hasNext()) {
            BulkImportPartition partition = importPartitions.next();
            Long conglom = partition.getConglomerateId();
            List<BulkImportPartition> partitionList = partitionMap.get(conglom);
            if (partitionList == null) {
                partitionList = Lists.newArrayList();
                partitionMap.put(conglom, partitionList);
            }
            partitionList.add(partition);
        }
    }
}
