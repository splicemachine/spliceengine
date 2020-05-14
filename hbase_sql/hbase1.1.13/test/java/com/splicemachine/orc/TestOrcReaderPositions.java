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

import com.splicemachine.orc.OrcTester.*;
import com.splicemachine.orc.metadata.Footer;
import com.splicemachine.orc.metadata.IntegerStatistics;
import com.splicemachine.orc.metadata.OrcMetadataReader;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.orc.impl.NullMemoryManager;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;
import static com.splicemachine.orc.OrcTester.Format.ORC_12;
import static com.splicemachine.orc.OrcTester.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.junit.Assert.assertEquals;

public class TestOrcReaderPositions
{
    public void testEntireFile()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());

            OrcRecordReader reader = createCustomOrcRecordReader(tempFile, new OrcMetadataReader(), OrcPredicate.TRUE, DataTypes.LongType);
            assertEquals(reader.getReaderRowCount(), 100);
            assertEquals(reader.getReaderPosition(), 0);
            assertEquals(reader.getFileRowCount(), reader.getReaderRowCount());
            assertEquals(reader.getFilePosition(), reader.getReaderPosition());

            for (int i = 0; i < 5; i++) {
                assertEquals(reader.nextBatch(), 20);
                assertEquals(reader.getReaderPosition(), i * 20L);
                assertEquals(reader.getFilePosition(), reader.getReaderPosition());
                assertCurrentBatch(reader, i);
            }

            assertEquals(reader.nextBatch(), -1);
            assertEquals(reader.getReaderPosition(), 100);
            assertEquals(reader.getFilePosition(), reader.getReaderPosition());
            reader.close();
        }
    }

    public void testStripeSkipping()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            createMultiStripeFile(tempFile.getFile());

            // test reading second and fourth stripes
            OrcPredicate predicate = (numberOfRows, statisticsByColumnIndex) -> {
                if (numberOfRows == 100) {
                    return true;
                }
                IntegerStatistics stats = statisticsByColumnIndex.get(0).getIntegerStatistics();
                return ((stats.getMin() == 60) && (stats.getMax() == 117)) ||
                        ((stats.getMin() == 180) && (stats.getMax() == 237));
            };

            OrcRecordReader reader = createCustomOrcRecordReader(tempFile, new OrcMetadataReader(), predicate, DataTypes.LongType);
            assertEquals(reader.getFileRowCount(), 100);
            assertEquals(reader.getReaderRowCount(), 40);
            assertEquals(reader.getFilePosition(), 0);
            assertEquals(reader.getReaderPosition(), 0);

            // second stripe
            assertEquals(reader.nextBatch(), 20);
            assertEquals(reader.getReaderPosition(), 0);
            assertEquals(reader.getFilePosition(), 20);
            assertCurrentBatch(reader, 1);

            // fourth stripe
            assertEquals(reader.nextBatch(), 20);
            assertEquals(reader.getReaderPosition(), 20);
            assertEquals(reader.getFilePosition(), 60);
            assertCurrentBatch(reader, 3);

            assertEquals(reader.nextBatch(), -1);
            assertEquals(reader.getReaderPosition(), 40);
            assertEquals(reader.getFilePosition(), 100);
            reader.close();
        }
    }

    public void testRowGroupSkipping()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            // create single strip file with multiple row groups
            int rowCount = 142_000;
            createSequentialFile(tempFile.getFile(), rowCount);

            // test reading two row groups from middle of file
            OrcPredicate predicate = (numberOfRows, statisticsByColumnIndex) -> {
                if (numberOfRows == rowCount) {
                    return true;
                }
                IntegerStatistics stats = statisticsByColumnIndex.get(0).getIntegerStatistics();
                return (stats.getMin() == 50_000) || (stats.getMin() == 60_000);
            };

            OrcRecordReader reader = createCustomOrcRecordReader(tempFile, new OrcMetadataReader(), predicate, DataTypes.LongType);

            assertEquals(reader.getFileRowCount(), rowCount);
            assertEquals(reader.getReaderRowCount(), rowCount);
            assertEquals(reader.getFilePosition(), 0);
            assertEquals(reader.getReaderPosition(), 0);

            long position = 50_000;
            while (true) {
                int batchSize = reader.nextBatch();
                if (batchSize == -1) {
                    break;
                }

                ColumnVector block = reader.readBlock(DataTypes.LongType, 0);
                for (int i = 0; i < batchSize; i++) {
                    assertEquals(block.getLong(i), position + i);
                }

                assertEquals(reader.getFilePosition(), position);
                assertEquals(reader.getReaderPosition(), position);
                position += batchSize;
            }

            assertEquals(position, 70_000);
            assertEquals(reader.getFilePosition(), rowCount);
            assertEquals(reader.getReaderPosition(), rowCount);
            reader.close();
        }
    }

    public void testReadUserMetadata()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            Map<String, String> metadata = ImmutableMap.of(
                    "a", "ala",
                    "b", "ma",
                    "c", "kota");
            createFileWithOnlyUserMetadata(tempFile.getFile(), metadata);

            OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, DataSize.Unit.MEGABYTE), new DataSize(1, DataSize.Unit.MEGABYTE), new DataSize(1, DataSize.Unit.MEGABYTE));
            OrcReader orcReader = new OrcReader(orcDataSource, new OrcMetadataReader(), new DataSize(1, DataSize.Unit.MEGABYTE), new DataSize(1, DataSize.Unit.MEGABYTE));
            Footer footer = orcReader.getFooter();
            Map<String, String> readMetadata = Maps.transformValues(footer.getUserMetadata(), Slice::toStringAscii);
            assertEquals(readMetadata, metadata);
        }
    }

    private static void assertCurrentBatch(OrcRecordReader reader, int stripe)
            throws IOException
    {
        ColumnVector block = reader.readBlock(DataTypes.LongType, 0);
        for (int i = 0; i < 20; i++) {
            assertEquals(block.getLong(i), ((stripe * 20L) + i) * 3);
        }
    }

    // write 5 stripes of 20 values each: (0,3,6,..,57), (60,..,117), .., (..297)
    private static void createMultiStripeFile(File file)
            throws IOException, ReflectiveOperationException, SerDeException
    {
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(file, ORC_12, OrcTester.Compression.NONE, javaLongObjectInspector);

        @SuppressWarnings("deprecation") Serializer serde = new OrcSerde();
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", javaLongObjectInspector);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 0; i < 300; i += 3) {
            if ((i > 0) && (i % 60 == 0)) {
                flushWriter(writer);
            }

            objectInspector.setStructFieldData(row, field, (long) i);
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }

        writer.close(false);
    }

    private static void createFileWithOnlyUserMetadata(File file, Map<String, String> metadata)
            throws IOException
    {
        Configuration conf = new Configuration();
        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
                // .memory(new NullMemoryManager(new Configuration()))
                .inspector(createSettableStructObjectInspector("test", javaLongObjectInspector))
                .compress(CompressionKind.SNAPPY);
        Writer writer = OrcFile.createWriter(new Path(file.toURI()), writerOptions);
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            writer.addUserMetadata(entry.getKey(), ByteBuffer.wrap(entry.getValue().getBytes(UTF_8)));
        }
        writer.close();
    }

    private static void flushWriter(FileSinkOperator.RecordWriter writer)
            throws IOException, ReflectiveOperationException
    {
        Field field = OrcOutputFormat.class.getClassLoader()
                .loadClass(OrcOutputFormat.class.getName() + "$OrcRecordWriter")
                .getDeclaredField("writer");
        field.setAccessible(true);
        ((Writer) field.get(writer)).writeIntermediateFooter();
    }

    private static void createSequentialFile(File file, int count)
            throws IOException, ReflectiveOperationException, SerDeException
    {
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(file, ORC_12, OrcTester.Compression.NONE, javaLongObjectInspector);

        @SuppressWarnings("deprecation") Serializer serde = new OrcSerde();
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", javaLongObjectInspector);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 0; i < count; i++) {
            objectInspector.setStructFieldData(row, field, (long) i);
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }

        writer.close(false);
    }
}
