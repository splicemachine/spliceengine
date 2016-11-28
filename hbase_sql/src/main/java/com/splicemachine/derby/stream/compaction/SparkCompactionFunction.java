/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.compaction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.iterators.EmptyListIterator;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.compactions.SpliceDefaultCompactor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.hbase.ReadOnlyHTableDescriptor;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.utils.SpliceLogUtils;
import scala.Tuple2;

public class SparkCompactionFunction extends SpliceFlatMapFunction<SpliceOperation,Iterator<Tuple2<Integer,Iterator>>,String> implements Externalizable {
    private static final Logger LOG = Logger.getLogger(SparkCompactionFunction.class);
    private long smallestReadPoint;
    private byte[] namespace;
    private byte[] tableName;
    private byte[] storeColumn;
    private HRegionInfo hri;
    private long mat;

    public SparkCompactionFunction() {

    }

    public SparkCompactionFunction(long smallestReadPoint,
                                   byte[] namespace,
                                   byte[] tableName,
                                   HRegionInfo hri,
                                   byte[] storeColumn) {
        this(smallestReadPoint, namespace, tableName, hri, storeColumn,0L);
    }

    public SparkCompactionFunction(long smallestReadPoint,
                                   byte[] namespace,
                                   byte[] tableName,
                                   HRegionInfo hri,
                                   byte[] storeColumn,
                                   long minimumActiveTxn) {
        this.smallestReadPoint = smallestReadPoint;
        this.namespace = namespace;
        this.tableName = tableName;
        this.hri = hri;
        this.storeColumn = storeColumn;
        this.mat = minimumActiveTxn;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] hriBytes = hri.toByteArray();
        super.writeExternal(out);
        out.writeLong(smallestReadPoint);
        out.writeInt(namespace.length);
        out.write(namespace);
        out.writeInt(tableName.length);
        out.write(tableName);
        out.writeInt(hriBytes.length);
        out.write(hriBytes);
        out.writeInt(storeColumn.length);
        out.write(storeColumn);
        out.writeLong(mat);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        smallestReadPoint = in.readLong();
        namespace = new byte[in.readInt()];
        in.readFully(namespace);
        tableName = new byte[in.readInt()];
        in.readFully(tableName);
        byte[] hriBytes = new byte[in.readInt()];
        in.readFully(hriBytes);
        try {
            hri = HRegionInfo.parseFrom(hriBytes);
        } catch (Exception e) {
            throw new IOException(e);
        }
        storeColumn = new byte[in.readInt()];
        in.readFully(storeColumn);
        mat = in.readLong();
        SpliceSpark.setupSpliceStaticComponents();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<String> call(Iterator it) throws Exception {

        ArrayList<StoreFile> readersToClose = new ArrayList<StoreFile>();
        Configuration conf = HConfiguration.unwrapDelegate();
        TableName tn = TableName.valueOf(namespace, tableName);
        PartitionFactory tableFactory=SIDriver.driver().getTableFactory();
        Table table = (((ClientPartition)tableFactory.getTable(tn)).unwrapDelegate());

        FileSystem fs = FSUtils.getCurrentFileSystem(conf);
        Path rootDir = FSUtils.getRootDir(conf);

        HTableDescriptor htd = table.getTableDescriptor();
        HRegion region = HRegion.openHRegion(conf, fs, rootDir, hri, new ReadOnlyHTableDescriptor(htd), null, null, null);
        Store store = region.getStore(storeColumn);

        assert it.hasNext();
        Tuple2 t = (Tuple2)it.next();
        Iterator files = (Iterator)t._2;
        if (LOG.isTraceEnabled()) {
            LOG.trace("compacting files: ");
        }
        while (files.hasNext()) {
            String file = (String)files.next();
            if (LOG.isTraceEnabled()) {
                LOG.trace(file + "\n");
            }
            readersToClose.add(
                    new StoreFile(
                            fs,
                            new Path(file),
                            conf,
                            store.getCacheConfig(),
                            store.getFamily().getBloomFilterType()
                    )
            );
        }

        SpliceDefaultCompactor sdc = new SpliceDefaultCompactor(conf, store, smallestReadPoint,mat);
        List<Path> paths = sdc.sparkCompact(new CompactionRequest(readersToClose));

        if (LOG.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder(100);
            sb.append(String.format("Result %d paths: ", paths.size()));
            for (Path path: paths) {
                sb.append(String.format("\nPath: %s", path));
            }
            SpliceLogUtils.trace(LOG, sb.toString());
        }
        return (paths == null || paths.isEmpty()) ?
                EmptyListIterator.INSTANCE:
            new SingletonIterator(paths.get(0).toString());
    }

}
