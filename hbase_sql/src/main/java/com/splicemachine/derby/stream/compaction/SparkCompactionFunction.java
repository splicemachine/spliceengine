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

package com.splicemachine.derby.stream.compaction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.splicemachine.compactions.SpliceDefaultCompactor;
import com.splicemachine.stream.SparkCompactionContext;
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
    private boolean isMajor;
    private SparkCompactionContext context;
    private InetSocketAddress[] favoredNodes;

    public SparkCompactionFunction() {

    }

    public SparkCompactionFunction(long smallestReadPoint, byte[] namespace,
                                   byte[] tableName, HRegionInfo hri, byte[] storeColumn, boolean isMajor, InetSocketAddress[] favoredNodes) {
        this.smallestReadPoint = smallestReadPoint;
        this.namespace = namespace;
        this.tableName = tableName;
        this.hri = hri;
        this.storeColumn = storeColumn;
        this.isMajor = isMajor;
        this.favoredNodes = favoredNodes;
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
        out.writeBoolean(isMajor);
        out.writeObject(context);
        out.writeInt(favoredNodes==null?0:favoredNodes.length);
        if (favoredNodes != null) {
            for (int i = 0; i< favoredNodes.length; i++) {
                out.writeObject(favoredNodes[i]);
            }
        }
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
        isMajor = in.readBoolean();
        context = (SparkCompactionContext) in.readObject();
        favoredNodes = new InetSocketAddress[in.readInt()];
        for (int i = 0; i< favoredNodes.length; i++) {
            favoredNodes[i] = (InetSocketAddress) in.readObject();
        }
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

        SpliceDefaultCompactor sdc = new SpliceDefaultCompactor(conf, store, smallestReadPoint);
        CompactionRequest compactionRequest = new CompactionRequest(readersToClose);
        compactionRequest.setIsMajor(isMajor, isMajor);
        List<Path> paths = sdc.sparkCompact(compactionRequest, context, favoredNodes);

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

    public void setContext(SparkCompactionContext context) {
        this.context = context;
    }
}
