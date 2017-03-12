package com.splicemachine.derby.stream.function;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.impl.driver.SIDriver;
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

    public RowKeyGenerator(String bulkImportDirectory,
                           long headConglom,
                           long indexConglom) {
        this.bulkImportDirectory = bulkImportDirectory;
        this.heapConglom = headConglom;
        this.indexConglom = indexConglom;
    }

    public RowKeyGenerator() {

    }

    public Iterable<String> call(Iterator<Tuple2<Long,Tuple2<byte[], byte[]>>> iterator) throws Exception {
        if (!initialized) {
            init();
            initialized = true;
        }
        while(iterator.hasNext()) {
            Tuple2<Long,Tuple2<byte[], byte[]>> t = iterator.next();
            Long conglomerateId = t._1;
            Tuple2<byte[], byte[]> kvPair = t._2;
            byte[] key = kvPair._1;
            if (indexConglom == -1 ||conglomerateId == indexConglom) {
                keys.add(key);
            }
        }
        outputKeys();
        return fileNames;
    }

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
