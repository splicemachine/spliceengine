package com.splicemachine.orc;

import com.splicemachine.hive.orc.HdfsOrcDataSource;
import com.splicemachine.orc.block.Block;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.metadata.OrcMetadataReader;
import com.splicemachine.spi.block.Block;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import java.net.URI;
import static com.splicemachine.spi.type.VarcharType.VARCHAR;
import static com.splicemachine.spi.type.IntegerType.INTEGER;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

/**
 * Created by jleach on 3/13/17.
 */

public class SpliceTestOrcReader {


    @Test
    public void testOrcReader() throws Exception {
        DataSize orcMaxMergeDistance = new DataSize(1, MEGABYTE);
        DataSize orcMaxBufferSize = new DataSize(8, MEGABYTE);
        DataSize orcStreamBufferSize = new DataSize(8, MEGABYTE);
        Configuration config = new Configuration();

        config.set("presto.s3.access-key","AKIAJ4YJKO4MWOFWPXKA");
        config.set("presto.s3.secret-key","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");

        config.set("fs.s3a.impl","com.splicemachine.hive.PrestoS3FileSystem");


        FileSystem fs = FileSystem.get(new URI("s3a://splice-qa/externaltables/TPCH100/orc/lineitem/"),
                config);
        Path path = new Path("s3a://splice-qa/externaltables/TPCH100/orc/lineitem/part-00000-c5f3a4a0-6f0b-4705-8708-dadff7f72a10.orc");
        long size = fs.getFileStatus(path).getLen();
        FSDataInputStream is = fs.open(path);
        OrcDataSource orcDataSource = new HdfsOrcDataSource(path.toString(), size, orcMaxMergeDistance,
                orcMaxBufferSize, orcStreamBufferSize, is);
        OrcPredicate orcPredicate;

        OrcReader reader = new OrcReader(orcDataSource,new OrcMetadataReader(), DataSize.succinctDataSize(16, DataSize.Unit.KILOBYTE),DataSize.succinctDataSize(16, DataSize.Unit.MEGABYTE));
        OrcRecordReader orcRecordReader =
        reader.createRecordReader(
                ImmutableMap.of(0, INTEGER),
                (numberOfRows, statisticsByColumnIndex) -> true,
                DateTimeZone.UTC,new AggregatedMemoryContext());
        int foo = orcRecordReader.nextBatch();
        Block block = orcRecordReader.readBlock(INTEGER,0);
        System.out.println(reader.getColumnNames());
        //OrcDataSource orcDataSource, MetadataReader metadataReader, DataSize maxMergeDistance, DataSize maxReadSize)
    }

}
