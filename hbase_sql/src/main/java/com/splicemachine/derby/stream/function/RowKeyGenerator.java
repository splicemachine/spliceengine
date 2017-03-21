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

    private Map<Long, List<byte[]>> keyMap;
    private List<String> fileNames;
    private boolean initialized;

    public RowKeyGenerator(String bulkImportDirectory) {
        this.bulkImportDirectory = bulkImportDirectory;
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
            List<byte[]> keys = keyMap.get(conglomerateId);
            if (keys == null) {
                keys = Lists.newArrayList();
                keyMap.put(conglomerateId, keys);
            }
            keys.add(key);
        }
        outputKeys();
        return fileNames.iterator();
    }

    private void outputKeys() throws IOException {
        BufferedWriter br = null;
        try {
            Configuration conf = HConfiguration.unwrapDelegate();
            FileSystem fs = FileSystem.get(URI.create(bulkImportDirectory), conf);

            for (Long conglomerateId : keyMap.keySet()) {
                List<byte[]> keys = keyMap.get(conglomerateId);
                Collections.sort(keys, new Comparator<byte[]>() {
                    @Override
                    public int compare(byte[] o1, byte[] o2) {
                        return Bytes.compareTo(o1, o2);
                    }
                });
                Path path = new Path(bulkImportDirectory, conglomerateId.toString());
                Path outFile = new Path(path, "keys");
                FSDataOutputStream os = fs.create(outFile);
                br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
                for (byte[] key : keys) {
                    br.write(Bytes.toStringBinary(key) + "\n");
                }
                br.close();
                fileNames.add(outFile.toString());
            }
        }
        finally {
            if (br != null)
                br.close();
        }
    }

    private void init() {
        keyMap = new HashMap<>();
        fileNames = Lists.newArrayList();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        try {
            out.writeUTF(bulkImportDirectory);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bulkImportDirectory = in.readUTF();
    }
}
