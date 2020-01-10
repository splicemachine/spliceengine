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

package com.splicemachine.derby.stream.function;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * Created by jyuan on 3/17/17.
 */
public class RowKeyGenerator <Op extends SpliceOperation>
        extends SpliceFlatMapFunction<SpliceBaseOperation, Iterator<Tuple2<Long,Tuple2<byte[], byte[]>>>, String> implements Serializable{

    private String bulkImportDirectory;

    private List<byte[]> keys;
    private List<String> fileNames;
    private boolean initialized;
    private long heapConglom;
    private long indexConglom;
    private boolean isUnique;

    public RowKeyGenerator(String bulkImportDirectory,
                           long headConglom,
                           long indexConglom,
                           boolean isUnique) {
        this.bulkImportDirectory = bulkImportDirectory;
        this.heapConglom = headConglom;
        this.indexConglom = indexConglom;
        this.isUnique = isUnique;
    }

    public RowKeyGenerator() {

    }

    public Iterator<String> call(Iterator<Tuple2<Long,Tuple2<byte[], byte[]>>> iterator) throws Exception {
        if (!initialized) {
            init();
            initialized = true;
        }

        while(iterator.hasNext()) {
            Tuple2<Long,Tuple2<byte[], byte[]>> t = iterator.next();
            Long conglomerateId = t._1;
            Tuple2<byte[], byte[]> kvPair = t._2;
            byte[] key = kvPair._1;
            if(conglomerateId == indexConglom) {
                if (!isUnique) {
                    // for non-unique index, remove rowkey of base table in the end
                    int endIndex = key.length-1;
                    while (endIndex > 0 && key[endIndex] != 0)
                        endIndex--;
                    key = Arrays.copyOf(key, endIndex);
                }
            }
            if (indexConglom == -1 ||conglomerateId == indexConglom) {
                keys.add(key);
            }
        }
        outputKeys();
        return fileNames.iterator();
    }

    /**
     * Sort keys and output in HBase escaped string format
     * @throws IOException
     */
    private void outputKeys() throws IOException {
        BufferedWriter br = null;
        try {
            Configuration conf = HConfiguration.unwrapDelegate();
            FileSystem fs = FileSystem.get(URI.create(bulkImportDirectory), conf);

            Collections.sort(keys, new Comparator<byte[]>() {
                @Override
                public int compare(byte[] o1, byte[] o2) {
                    return Bytes.compareTo(o1, o2);
                }
            });
            long conglom = indexConglom == -1 ? heapConglom : indexConglom;
            Path path = new Path(bulkImportDirectory, new Long(conglom).toString());
            Path outFile = new Path(path, "keys");
            FSDataOutputStream os = fs.create(outFile);
            br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            for (byte[] key : keys) {
                br.write(Bytes.toStringBinary(key) + "\n");
            }
            br.close();
            fileNames.add(outFile.toString());

        }
        finally {
            if (br != null)
                br.close();
        }
    }

    private void init() {
        keys = Lists.newArrayList();
        fileNames = Lists.newArrayList();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(bulkImportDirectory);
        out.writeLong(heapConglom);
        out.writeLong(indexConglom);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bulkImportDirectory = in.readUTF();
        heapConglom = in.readLong();
        indexConglom = in.readLong();
    }
}
