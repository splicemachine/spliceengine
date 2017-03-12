package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 * Created by jleach on 3/1/17.
 */
public class HFileFlatMapFunction extends SpliceFlatMapFunction<SpliceBaseOperation, Iterator<KVPair>, KVPair> {
        private static final long serialVersionUID = 844136943916989111L;
        private final byte [] now = Bytes.toBytes(System.currentTimeMillis());
        private String outputDir = "/tmp/hbase";
        private int txnID = 100;

        public HFileFlatMapFunction() {
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
        }

        @Override
        public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException {
        }

        @SuppressWarnings("unchecked")
        @Override
        public Iterator<KVPair> call(Iterator<KVPair> pairs) throws Exception {
            StoreFile.Writer writer = getNewWriter(SIConstants.DEFAULT_FAMILY_BYTES,HBaseConfiguration.create());
            while (pairs.hasNext()) {
                KVPair pair = pairs.next();
                KeyValue kv = new KeyValue(pair.getRowKey(), SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES, txnID, pair.getValue());
                writer.append(kv);
            }
            close(writer);
            return pairs; // Do Not Mutate Baby
        }

    private void close(final StoreFile.Writer w) throws IOException {
        if (w != null) {
            w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
                    Bytes.toBytes(System.currentTimeMillis()));
            w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
                    Bytes.toBytes("bulk load"));//context.getTaskAttemptID().toString())); TODO JL
            w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
                    Bytes.toBytes(true));
            w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
                    Bytes.toBytes(false));
            w.appendTrackedTimestampsToMetadata();
            w.close();
        }
    }

    private StoreFile.Writer getNewWriter(byte[] family, Configuration conf)
            throws IOException {
        Path familydir = new Path(outputDir, Bytes.toString(family));
        Compression.Algorithm compression = Compression.Algorithm.NONE;
        BloomType bloomType = BloomType.ROW;
        Integer blockSize = HConstants.DEFAULT_BLOCKSIZE;
        DataBlockEncoding encoding = DataBlockEncoding.NONE;
        Configuration tempConf = new Configuration(conf);
        tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
        HFileContextBuilder contextBuilder = new HFileContextBuilder()
                .withCompression(compression)
                .withChecksumType(HStore.getChecksumType(conf))
                .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
                .withBlockSize(blockSize);

        if (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
            contextBuilder.withIncludesTags(true);
        }

        contextBuilder.withDataBlockEncoding(encoding);
        HFileContext hFileContext = contextBuilder.build();
        try {
            return new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf), FileSystem.get(HBaseConfiguration.create())) // TODO FIX JL
                    .withOutputDir(familydir).withBloomType(bloomType)
                    .withComparator(KeyValue.COMPARATOR)
                    .withFileContext(hFileContext).build();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }


}
