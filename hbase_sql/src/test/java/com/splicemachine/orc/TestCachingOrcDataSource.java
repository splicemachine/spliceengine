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
package com.splicemachine.orc;

import com.splicemachine.orc.OrcTester.Compression;
import com.splicemachine.orc.OrcTester.Format;
import com.splicemachine.orc.OrcTester.TempFile;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.metadata.OrcMetadataReader;
import com.splicemachine.orc.metadata.StripeInformation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.splicemachine.orc.TestingOrcDataSource;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.junit.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;
import static com.splicemachine.orc.OrcRecordReader.LinearProbeRangeFinder.createTinyStripesRangeFinder;
import static com.splicemachine.orc.OrcRecordReader.wrapWithCacheIfTinyStripes;
import static com.splicemachine.orc.OrcTester.Compression.NONE;
import static com.splicemachine.orc.OrcTester.Compression.ZLIB;
import static com.splicemachine.orc.OrcTester.Format.ORC_12;
import static com.splicemachine.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.splicemachine.orc.OrcTester.writeOrcFileColumnOld;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestCachingOrcDataSource
{
    private static final int POSITION_COUNT = 50000;

    private static TempFile tempFile;

    @BeforeClass
    public static void setUp()
            throws Exception
    {
        tempFile = new TempFile();
        Random random = new Random();
        Iterator<String> iterator = Stream.generate(() -> Long.toHexString(random.nextLong())).limit(POSITION_COUNT).iterator();
        writeOrcFileColumnOld(tempFile.getFile(), ORC_12, createOrcRecordWriter(tempFile.getFile(), ORC_12, ZLIB, javaStringObjectInspector), javaStringObjectInspector, iterator);
    }

    @AfterClass
    public static void tearDown()
            throws Exception
    {
        tempFile.close();
    }

    @Test
    public void testWrapWithCacheIfTinyStripes()
            throws IOException
    {
        DataSize maxMergeDistance = new DataSize(1, Unit.MEGABYTE);
        DataSize maxReadSize = new DataSize(8, Unit.MEGABYTE);

        OrcDataSource actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(),
                maxMergeDistance,
                maxReadSize);
        Assert.assertTrue(actual instanceof CachingOrcDataSource);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10)),
                maxMergeDistance,
                maxReadSize);
        Assert.assertTrue(actual instanceof CachingOrcDataSource);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 10, 10, 10)),
                maxMergeDistance,
                maxReadSize);
        Assert.assertTrue(actual instanceof CachingOrcDataSource);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10)),
                maxMergeDistance,
                maxReadSize);
        Assert.assertTrue(actual instanceof CachingOrcDataSource);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20 + 1, 10, 10)),
                maxMergeDistance,
                maxReadSize);
        assertNotInstanceOf(actual, CachingOrcDataSource.class);
    }

    @Test
    public void testTinyStripesReadCacheAt()
            throws IOException
    {
        DataSize maxMergeDistance = new DataSize(1, Unit.MEGABYTE);
        DataSize maxReadSize = new DataSize(8, Unit.MEGABYTE);

        TestingOrcDataSource testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        CachingOrcDataSource cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10)),
                        maxMergeDistance,
                        maxReadSize));
        cachingOrcDataSource.readCacheAt(3);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(3, 60)));
        cachingOrcDataSource.readCacheAt(63);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(63, 8 * 1048576)));

        testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10)),
                        maxMergeDistance,
                        maxReadSize));
        cachingOrcDataSource.readCacheAt(62); // read at the end of a stripe
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(3, 60)));
        cachingOrcDataSource.readCacheAt(63);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(63, 8 * 1048576)));

        testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(new StripeInformation(123, 3, 1, 0, 0), new StripeInformation(123, 4, 1048576, 1048576, 1048576 * 3), new StripeInformation(123, 4 + 1048576 * 5, 1048576, 1048576, 1048576)),
                        maxMergeDistance,
                        maxReadSize));
        cachingOrcDataSource.readCacheAt(3);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(3, 1 + 1048576 * 5)));
        cachingOrcDataSource.readCacheAt(4 + 1048576 * 5);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(4 + 1048576 * 5, 3 * 1048576)));
    }

    public void testIntegration()
            throws IOException
    {
        // tiny file
        TestingOrcDataSource orcDataSource = new TestingOrcDataSource(
                new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE)));
        doIntegration(orcDataSource, new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE));
        assertEquals(orcDataSource.getReadCount(), 1); // read entire file at once

        // tiny stripes
        orcDataSource = new TestingOrcDataSource(
                new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE)));
        doIntegration(orcDataSource, new DataSize(400, Unit.KILOBYTE), new DataSize(400, Unit.KILOBYTE));
        assertEquals(orcDataSource.getReadCount(), 3); // footer, first few stripes, last few stripes
    }

    public void doIntegration(TestingOrcDataSource orcDataSource, DataSize maxMergeDistance, DataSize maxReadSize)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(orcDataSource, new OrcMetadataReader(), maxMergeDistance, maxReadSize);
        // 1 for reading file footer
        assertEquals(orcDataSource.getReadCount(), 1);
        List<StripeInformation> stripes = orcReader.getFooter().getStripes();
        // Sanity check number of stripes. This can be three or higher because of orc writer low memory mode.
        Assert.assertTrue(stripes.size() > 1);
        //verify wrapped by CachingOrcReader
        Assert.assertTrue(wrapWithCacheIfTinyStripes(orcDataSource, stripes, maxMergeDistance, maxReadSize) instanceof CachingOrcDataSource);

        OrcRecordReader orcRecordReader = orcReader.createRecordReader(
                ImmutableMap.of(0, DataTypes.StringType),
                (numberOfRows, statisticsByColumnIndex) -> true,
                HIVE_STORAGE_TIME_ZONE,
                new AggregatedMemoryContext(),Collections.EMPTY_LIST,Collections.EMPTY_LIST);
        int positionCount = 0;
        while (true) {
            int batchSize = orcRecordReader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            ColumnVector block = orcRecordReader.readBlock(DataTypes.StringType, 0);
            positionCount += block.getElementsAppended();
        }
        assertEquals(positionCount, POSITION_COUNT);
    }

    public static <T, U extends T> void assertNotInstanceOf(T actual, Class<U> expectedType)
    {
        assertNotNull("actual is null",actual);
        assertNotNull("expectedType is null",expectedType);
        if (expectedType.isInstance(actual)) {
            fail(String.format("expected:<%s> to not be an instance of <%s>", actual, expectedType.getName()));
        }
    }

    private static FileSinkOperator.RecordWriter createOrcRecordWriter(File outputFile, Format format, Compression compression, ObjectInspector columnObjectInspector)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        jobConf.set("hive.exec.orc.write.format", format == ORC_12 ? "0.12" : "0.11");
        jobConf.set("hive.exec.orc.default.compress", compression.name());

        Properties tableProperties = new Properties();
        tableProperties.setProperty("columns", "test");
        tableProperties.setProperty("columns.types", columnObjectInspector.getTypeName());
        tableProperties.setProperty("orc.stripe.size", "1200000");

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compression != NONE,
                tableProperties,
                () -> { });
    }

    private static class FakeOrcDataSource
            implements OrcDataSource
    {
        public static final FakeOrcDataSource INSTANCE = new FakeOrcDataSource();

        @Override
        public long getReadBytes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getReadTimeNanos()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getSize()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(long position, byte[] buffer)
                throws IOException
        {
            // do nothing
        }

        @Override
        public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            // do nothing
        }

        @Override
        public <K> Map<K, FixedLengthSliceInput> readFully(Map<K, DiskRange> diskRanges)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }
    }
}
