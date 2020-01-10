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

import com.google.common.base.Preconditions;
import com.splicemachine.orc.block.BlockFactory;
import com.splicemachine.orc.block.ColumnBlock;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.metadata.DwrfMetadataReader;
import com.splicemachine.orc.metadata.MetadataReader;
import com.splicemachine.orc.metadata.OrcMetadataReader;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.StructField;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.Map.Entry;

import static com.splicemachine.orc.OrcTester.Compression.NONE;
import static com.splicemachine.orc.OrcTester.Compression.ZLIB;
import static com.splicemachine.orc.OrcTester.Format.DWRF;
import static com.splicemachine.orc.OrcTester.Format.ORC_12;
import static com.splicemachine.orc.OrcTester.Format.ORC_11;
import static com.google.common.base.Functions.constant;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.*;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;
import static org.junit.Assert.*;

public class OrcTester
{
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.getDefault();

    public enum Format
    {
        ORC_12, ORC_11, DWRF
    }

    public enum Compression
    {
        ZLIB, SNAPPY, NONE
    }

    private boolean structTestsEnabled;
    private boolean mapTestsEnabled;
    private boolean listTestsEnabled;
    private boolean complexStructuralTestsEnabled;
    private boolean structuralNullTestsEnabled;
    private boolean reverseTestsEnabled;
    private boolean nullTestsEnabled;
    private boolean skipBatchTestsEnabled;
    private boolean skipStripeTestsEnabled;
    private Set<Format> formats = ImmutableSet.of();
    private Set<Compression> compressions = ImmutableSet.of();

    public static OrcTester quickOrcTester()
    {
        OrcTester orcTester = new OrcTester();
        orcTester.structTestsEnabled = true;
//        orcTester.mapTestsEnabled = true; // ENABLE MAP Streams JL
        orcTester.mapTestsEnabled = false;
        orcTester.listTestsEnabled = true;
        orcTester.nullTestsEnabled = true;
        orcTester.skipBatchTestsEnabled = true;
        orcTester.formats = ImmutableSet.of(ORC_12);
        orcTester.compressions = ImmutableSet.of(ZLIB);
        return orcTester;
    }

    public static OrcTester fullOrcTester()
    {
        OrcTester orcTester = new OrcTester();
        orcTester.structTestsEnabled = true;
//        orcTester.mapTestsEnabled = true;
        orcTester.mapTestsEnabled = false;
        orcTester.listTestsEnabled = true;
        orcTester.complexStructuralTestsEnabled = true;
        orcTester.structuralNullTestsEnabled = true;
        orcTester.reverseTestsEnabled = true;
        orcTester.nullTestsEnabled = true;
        orcTester.skipBatchTestsEnabled = true;
        orcTester.skipStripeTestsEnabled = true;
        orcTester.formats = ImmutableSet.copyOf(new Format[]{ORC_12,ORC_11});
        orcTester.compressions = ImmutableSet.copyOf(Compression.values());
        return orcTester;
    }

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> readValues, DataType type)
            throws Exception
    {
        // just the values
        testRoundTripType(objectInspector, readValues, type);

        // all nulls
        assertRoundTrip(objectInspector, transform(readValues, constant(null)), type);

        // values wrapped in struct
        if (structTestsEnabled) {
            testStructRoundTrip(objectInspector, readValues, type);
        }

        // values wrapped in a struct wrapped in a struct
        if (complexStructuralTestsEnabled) {
            testStructRoundTrip(createHiveStructInspector(objectInspector),
                    transform(readValues, OrcTester::toHiveStruct),
                    rowType(type, type, type));
        }

        // values wrapped in map
        if (mapTestsEnabled) {
            testMapRoundTrip(objectInspector, readValues, type);
        }

        // values wrapped in list
        if (listTestsEnabled) {
            testListRoundTrip(objectInspector, readValues, type);
        }

        // values wrapped in a list wrapped in a list
        if (complexStructuralTestsEnabled) {
            testListRoundTrip(createHiveListInspector(objectInspector),
                    transform(readValues, OrcTester::toHiveList),
                    arrayType(type));
        }
    }

    private void testStructRoundTrip(ObjectInspector objectInspector, Iterable<?> readValues, DataType elementType)
            throws Exception
    {
        DataType rowType = rowType(elementType, elementType, elementType);
        // values in simple struct
        testRoundTripType(createHiveStructInspector(objectInspector),
                transform(readValues, OrcTester::toHiveStruct),
                rowType);

        if (structuralNullTestsEnabled) {
            // values and nulls in simple struct
            testRoundTripType(createHiveStructInspector(objectInspector),
                    transform(insertNullEvery(5, readValues), OrcTester::toHiveStruct),
                    rowType);

            // all null values in simple struct
            testRoundTripType(createHiveStructInspector(objectInspector),
                    transform(transform(readValues, constant(null)), OrcTester::toHiveStruct),
                    rowType);
        }
    }

    private void testMapRoundTrip(ObjectInspector objectInspector, Iterable<?> readValues, DataType elementType)
            throws Exception
    {
        DataType mapType = mapType(elementType, elementType);

        // maps can not have a null key, so select a value to use for the map key when the value is null
        Object readNullKeyValue = Iterables.getLast(readValues);

        // values in simple map
        testRoundTripType(createHiveMapInspector(objectInspector),
                transform(readValues, value -> toHiveMap(value, readNullKeyValue)),
                mapType);

        if (structuralNullTestsEnabled) {
            // values and nulls in simple map
            testRoundTripType(createHiveMapInspector(objectInspector),
                    transform(insertNullEvery(5, readValues), value -> toHiveMap(value, readNullKeyValue)),
                    mapType);

            // all null values in simple map
            testRoundTripType(createHiveMapInspector(objectInspector),
                    transform(transform(readValues, constant(null)), value -> toHiveMap(value, readNullKeyValue)),
                    mapType);
        }
    }

    private void testListRoundTrip(ObjectInspector objectInspector, Iterable<?> readValues, DataType elementType)
            throws Exception
    {
        DataType arrayType = arrayType(elementType);
        // values in simple list
        testRoundTripType(createHiveListInspector(objectInspector),
                transform(readValues, OrcTester::toHiveList),
                arrayType);

        if (structuralNullTestsEnabled) {
            // values and nulls in simple list
            testRoundTripType(createHiveListInspector(objectInspector),
                    transform(insertNullEvery(5, readValues), OrcTester::toHiveList),
                    arrayType);

            // all null values in simple list
            testRoundTripType(createHiveListInspector(objectInspector),
                    transform(transform(readValues, constant(null)), OrcTester::toHiveList),
                    arrayType);
        }
    }

    private void testRoundTripType(ObjectInspector objectInspector, Iterable<?> readValues, DataType type)
            throws Exception
    {
        // forward order
        assertRoundTrip(objectInspector, readValues, type);

        // reverse order
        if (reverseTestsEnabled) {
            assertRoundTrip(objectInspector, reverse(readValues), type);
        }

        if (nullTestsEnabled) {
            // forward order with nulls
            assertRoundTrip(objectInspector, insertNullEvery(5, readValues), type);

            // reverse order with nulls
            if (reverseTestsEnabled) {
                assertRoundTrip(objectInspector, insertNullEvery(5, reverse(readValues)), type);
            }
        }
    }

    public void assertRoundTrip(ObjectInspector objectInspector, Iterable<?> readValues, DataType type)
            throws Exception
    {
        for (Format formatVersion : formats) {
            MetadataReader metadataReader;
            if (DWRF == formatVersion) {
                if (hasType(objectInspector, PrimitiveCategory.DATE)) {
                    // DWRF doesn't support dates
                    return;
                }
                if (hasType(objectInspector, PrimitiveCategory.DECIMAL)) {
                    // DWRF doesn't support decimals
                    return;
                }
                if (hasType(objectInspector, PrimitiveCategory.CHAR)) {
                    // DWRF doesn't support chars
                    return;
                }
                metadataReader = new DwrfMetadataReader();
            }
            else {
                metadataReader = new OrcMetadataReader();
            }
            for (Compression compression : compressions) {
                try (TempFile tempFile = new TempFile()) {
                    writeOrcColumn(tempFile.getFile(), formatVersion, compression, objectInspector, readValues.iterator());

                    assertFileContents(objectInspector, tempFile, readValues, false, false, metadataReader, type);

                    if (skipBatchTestsEnabled) {
                        assertFileContents(objectInspector, tempFile, readValues, true, false, metadataReader, type);
                    }

                    if (skipStripeTestsEnabled) {
                        assertFileContents(objectInspector, tempFile, readValues, false, true, metadataReader, type);
                    }
                }
            }
        }
    }

    private static void assertFileContents(ObjectInspector objectInspector,
            TempFile tempFile,
            Iterable<?> expectedValues,
            boolean skipFirstBatch,
            boolean skipStripe,
            MetadataReader metadataReader,
            DataType type)
            throws IOException
    {
        OrcRecordReader recordReader = createCustomOrcRecordReader(tempFile, metadataReader, OrcPredicate.TRUE, type);
        assertEquals(recordReader.getReaderPosition(), 0);
        assertEquals(recordReader.getFilePosition(), 0);

        boolean isFirst = true;
        int rowsProcessed = 0;
        Iterator<?> iterator = expectedValues.iterator();
        for (int batchSize = toIntExact(recordReader.nextBatch()); batchSize >= 0; batchSize = toIntExact(recordReader.nextBatch())) {
            if (skipStripe && rowsProcessed < 10000) {
                assertEquals(advance(iterator, batchSize), batchSize);
            }
            else if (skipFirstBatch && isFirst) {
                assertEquals(advance(iterator, batchSize), batchSize);
                isFirst = false;
            }
            else {
                ColumnVector vector = recordReader.readBlock(type, 0);
                ColumnBlock block = BlockFactory.getColumnBlock(vector,type);
                List<Object> data = new ArrayList<>(vector.getElementsAppended());
                for (int position = 0; position < vector.getElementsAppended(); position++) {
                    data.add(block.getTestObject(position));
                }
                for (int i = 0; i < batchSize; i++) {
                    assertTrue(iterator.hasNext());
                    Object expected = iterator.next();
                    Object actual = data.get(i);
                    assertColumnValueEquals(type, actual, expected);
                }
            }
            assertEquals(recordReader.getReaderPosition(), rowsProcessed);
            assertEquals(recordReader.getFilePosition(), rowsProcessed);
            rowsProcessed += batchSize;
        }
        assertFalse(iterator.hasNext());

        assertEquals(recordReader.getReaderPosition(), rowsProcessed);
        assertEquals(recordReader.getFilePosition(), rowsProcessed);
        recordReader.close();
    }

    private static void assertColumnValueEquals(DataType type, Object actual, Object expected)
    {
        if (actual == null) {
            assertNull(expected);
            return;
        }
        if (type instanceof ArrayType) {
            List<?> actualArray = (List<?>) actual;
            List<?> expectedArray = (List<?>) expected;
            assertEquals(actualArray.size(), expectedArray.size());
            DataType elementType = ((ArrayType)type).elementType();
            for (int i = 0; i < actualArray.size(); i++) {
                Object actualElement = actualArray.get(i);
                Object expectedElement = expectedArray.get(i);
                assertColumnValueEquals(elementType, actualElement, expectedElement);
            }
        }
        else if (type instanceof MapType) {
            Map<?, ?> actualMap = (Map<?, ?>) actual;
            Map<?, ?> expectedMap = (Map<?, ?>) expected;
            assertEquals(actualMap.size(), expectedMap.size());

            DataType keyType = ((MapType) type).keyType();
            DataType valueType = ((MapType) type).valueType();

            List<Entry<?, ?>> expectedEntries = new ArrayList<>(expectedMap.entrySet());
            for (Entry<?, ?> actualEntry : actualMap.entrySet()) {
                Iterator<Entry<?, ?>> iterator = expectedEntries.iterator();
                while (iterator.hasNext()) {
                    Entry<?, ?> expectedEntry = iterator.next();
                    try {
                        assertColumnValueEquals(keyType, actualEntry.getKey(), expectedEntry.getKey());
                        assertColumnValueEquals(valueType, actualEntry.getValue(), expectedEntry.getValue());
                        iterator.remove();
                    }
                    catch (AssertionError ignored) {
                    }
                }
            }
            assertTrue("Unmatched entries " + expectedEntries,expectedEntries.isEmpty());
        }
        else if (type instanceof StructType) {

            StructField[] fieldTypes = ((StructType)type).fields();

            List<?> actualRow = (List<?>) actual;
            List<?> expectedRow = (List<?>) expected;
            assertEquals(actualRow.size(), fieldTypes.length);
            assertEquals(actualRow.size(), expectedRow.size());

            for (int fieldId = 0; fieldId < actualRow.size(); fieldId++) {
                DataType fieldType = fieldTypes[fieldId].dataType();
                Object actualElement = actualRow.get(fieldId);
                Object expectedElement = expectedRow.get(fieldId);
                assertColumnValueEquals(fieldType, actualElement, expectedElement);
            }
        }
        else if (type instanceof DoubleType) {
            Double actualDouble = (Double) actual;
            Double expectedDouble = (Double) expected;
            if (actualDouble.isNaN()) {
                assertTrue("expected double to be NaN",expectedDouble.isNaN());
            }
            else {
                assertEquals(actualDouble, expectedDouble, 0.001);
            }
        }
        else if (!Objects.equals(actual, expected)) {
            assertEquals(expected, actual);
        }
    }

    static OrcRecordReader createCustomOrcRecordReader(TempFile tempFile, MetadataReader metadataReader, OrcPredicate predicate, DataType type)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE));
        OrcReader orcReader = new OrcReader(orcDataSource, metadataReader, new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE));

        assertEquals(orcReader.getColumnNames(), ImmutableList.of("test"));
        assertEquals(orcReader.getFooter().getRowsInRowGroup(), 10_000);

        return orcReader.createRecordReader(ImmutableMap.of(0, type), predicate, HIVE_STORAGE_TIME_ZONE, new AggregatedMemoryContext(),
                Collections.EMPTY_LIST,Collections.EMPTY_LIST);
    }

    static DataSize writeOrcColumn(File outputFile, Format format, Compression compression, ObjectInspector columnObjectInspector, Iterator<?> values)
            throws Exception
    {
        RecordWriter recordWriter;
        if (DWRF == format) {
            recordWriter = createDwrfRecordWriter(outputFile, compression, columnObjectInspector);
        }
        else {
            recordWriter = createOrcRecordWriter(outputFile, format, compression, columnObjectInspector);
        }
        return writeOrcFileColumnOld(outputFile, format, recordWriter, columnObjectInspector, values);
    }

    public static DataSize writeOrcFileColumnOld(File outputFile, Format format, RecordWriter recordWriter, ObjectInspector columnObjectInspector, Iterator<?> values)
            throws Exception
    {
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", columnObjectInspector);
        Object row = objectInspector.create();

        List<org.apache.hadoop.hive.serde2.objectinspector.StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());

        int i = 0;
        TypeInfo typeInfo = getTypeInfoFromTypeString(columnObjectInspector.getTypeName());
        while (values.hasNext()) {
            Object value = values.next();
            value = preprocessWriteValueOld(typeInfo, value);
            objectInspector.setStructFieldData(row, fields.get(0), value);

            @SuppressWarnings("deprecation") Serializer serde;
            if (DWRF == format) {
                serde = new org.apache.hadoop.hive.ql.io.orc.OrcSerde();
                if (i == 142_345) {
                    setDwrfLowMemoryFlag(recordWriter);
                }
            }
            else {
                serde = new OrcSerde();
            }
            Writable record = serde.serialize(row, objectInspector);
            recordWriter.write(record);
            i++;
        }

        recordWriter.close(false);
        return succinctBytes(outputFile.length());
    }

    private static Object preprocessWriteValueOld(TypeInfo typeInfo, Object value) throws IOException
    {
        if (value == null) {
            return null;
        }
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
                switch (primitiveCategory) {
                    case BOOLEAN:
                        return value;
                    case BYTE:
                        return ((Number) value).byteValue();
                    case SHORT:
                        return ((Number) value).shortValue();
                    case INT:
                        return ((Number) value).intValue();
                    case LONG:
                        return ((Number) value).longValue();
                    case FLOAT:
                        return ((Number) value).floatValue();
                    case DOUBLE:
                        return ((Number) value).doubleValue();
                    case DECIMAL:
                        return HiveDecimal.create(((Decimal) value).toBigDecimal().bigDecimal());
                    case STRING:
                        return value;
                    case CHAR:
                        return new HiveChar(value.toString(), ((CharTypeInfo) typeInfo).getLength());
                    case DATE:
                        LocalDate localDate = LocalDate.ofEpochDay((int)value);
                        ZonedDateTime zonedDateTime = localDate.atStartOfDay(ZoneId.systemDefault());

                        long millis = zonedDateTime.toEpochSecond() * 1000;
                        Date date = new Date(0);
                        // mills must be set separately to avoid masking
                        date.setTime(millis);
                        return date;
                    case TIMESTAMP:
                        long millisUtc = ((Long)value).intValue();
                        return new Timestamp(millisUtc);
                    case BINARY:
                        return ((String) value).getBytes();
//                        return (byte[])value;
                }
                break;
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                TypeInfo keyTypeInfo = mapTypeInfo.getMapKeyTypeInfo();
                TypeInfo valueTypeInfo = mapTypeInfo.getMapValueTypeInfo();
                Map<Object, Object> newMap = new HashMap<>();
                for (Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    newMap.put(preprocessWriteValueOld(keyTypeInfo, entry.getKey()), preprocessWriteValueOld(valueTypeInfo, entry.getValue()));
                }
                return newMap;
            case LIST:
                ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                TypeInfo elementTypeInfo = listTypeInfo.getListElementTypeInfo();
                List<Object> newList = new ArrayList<>(((Collection<?>) value).size());
                for (Object element : (Iterable<?>) value) {
                    newList.add(preprocessWriteValueOld(elementTypeInfo, element));
                }
                return newList;
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                List<?> fieldValues = (List<?>) value;
                List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
                List<Object> newStruct = new ArrayList<>();
                for (int fieldId = 0; fieldId < fieldValues.size(); fieldId++) {
                    newStruct.add(preprocessWriteValueOld(fieldTypeInfos.get(fieldId), fieldValues.get(fieldId)));
                }
                return newStruct;
        }
        throw new IOException(format("Unsupported Hive type: %s", typeInfo));
    }

    private static void setDwrfLowMemoryFlag(RecordWriter recordWriter)
    {
        Object writer = getFieldValue(recordWriter, "writer");
        Object memoryManager = getFieldValue(writer, "memoryManager");
        setFieldValue(memoryManager, "lowMemoryMode", true);
        try {
            writer.getClass().getMethod("enterLowMemoryMode").invoke(writer);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static Object getFieldValue(Object instance, String name)
    {
        try {
            Field writerField = instance.getClass().getDeclaredField(name);
            writerField.setAccessible(true);
            return writerField.get(instance);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static void setFieldValue(Object instance, String name, Object value)
    {
        try {
            Field writerField = instance.getClass().getDeclaredField(name);
            writerField.setAccessible(true);
            writerField.set(instance, value);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    static RecordWriter createOrcRecordWriter(File outputFile, Format format, Compression compression, ObjectInspector columnObjectInspector)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        jobConf.set("hive.exec.orc.write.format", format == ORC_12 ? "0.12" : "0.11");
        jobConf.set("hive.exec.orc.default.compress", compression.name());

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compression != NONE,
                createTableProperties("test", columnObjectInspector.getTypeName()),
                () -> { }
        );
    }

    private static RecordWriter createDwrfRecordWriter(File outputFile, Compression compressionCodec, ObjectInspector columnObjectInspector)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        jobConf.set("hive.exec.orc.default.compress", compressionCodec.name());
        jobConf.set("hive.exec.orc.compress", compressionCodec.name());
        OrcConf.setIntVar(jobConf, OrcConf.ConfVars.HIVE_ORC_ENTROPY_STRING_THRESHOLD, 1);
        OrcConf.setIntVar(jobConf, OrcConf.ConfVars.HIVE_ORC_DICTIONARY_ENCODING_INTERVAL, 2);
        OrcConf.setBoolVar(jobConf, OrcConf.ConfVars.HIVE_ORC_BUILD_STRIDE_DICTIONARY, true);
        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compressionCodec != NONE,
                createTableProperties("test", columnObjectInspector.getTypeName()),
                () -> { }
        );
    }

    static SettableStructObjectInspector createSettableStructObjectInspector(String name, ObjectInspector objectInspector)
    {
        return getStandardStructObjectInspector(ImmutableList.of(name), ImmutableList.of(objectInspector));
    }

    private static Properties createTableProperties(String name, String type)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty("columns", name);
        orderTableProperties.setProperty("columns.types", type);
        return orderTableProperties;
    }

    static class TempFile
            implements Closeable
    {
        private final File tempDir;
        private final File file;

        public TempFile()
        {
            tempDir = createTempDir();
            tempDir.mkdirs();
            file = new File(tempDir, "data.rcfile");
        }

        public File getFile()
        {
            return file;
        }

        @Override
        public void close()
        {
            FileUtils.deleteQuietly(tempDir);
            // hadoop creates crc files that must be deleted also, so just delete the whole directory
//            deleteRecursively(tempDir);
        }
    }

    private static <T> Iterable<T> reverse(Iterable<T> iterable)
    {
        return Lists.reverse(ImmutableList.copyOf(iterable));
    }

    private static <T> Iterable<T> insertNullEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                position++;
                if (position > n) {
                    position = 0;
                    return null;
                }

                if (!delegate.hasNext()) {
                    return endOfData();
                }

                return delegate.next();
            }
        };
    }

    private static StandardStructObjectInspector createHiveStructInspector(ObjectInspector objectInspector)
    {
        return getStandardStructObjectInspector(ImmutableList.of("a", "b", "c"), ImmutableList.of(objectInspector, objectInspector, objectInspector));
    }

    private static List<Object> toHiveStruct(Object input)
    {
        return asList(input, input, input);
    }

    private static StandardMapObjectInspector createHiveMapInspector(ObjectInspector objectInspector)
    {
        return getStandardMapObjectInspector(objectInspector, objectInspector);
    }

    private static Map<Object, Object> toHiveMap(Object input, Object nullKeyValue)
    {
        Map<Object, Object> map = new HashMap<>();
        map.put(input != null ? input : nullKeyValue, input);
        return map;
    }

    private static StandardListObjectInspector createHiveListInspector(ObjectInspector objectInspector)
    {
        return getStandardListObjectInspector(objectInspector);
    }

    private static List<Object> toHiveList(Object input)
    {
        return asList(input, input, input, input);
    }

    private static boolean hasType(ObjectInspector objectInspector, PrimitiveCategory... types)
    {
        if (objectInspector instanceof PrimitiveObjectInspector) {
            PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) objectInspector;
            PrimitiveCategory primitiveCategory = primitiveInspector.getPrimitiveCategory();
            for (PrimitiveCategory type : types) {
                if (primitiveCategory == type) {
                    return true;
                }
            }
            return false;
        }
        if (objectInspector instanceof ListObjectInspector) {
            ListObjectInspector listInspector = (ListObjectInspector) objectInspector;
            return hasType(listInspector.getListElementObjectInspector(), types);
        }
        if (objectInspector instanceof MapObjectInspector) {
            MapObjectInspector mapInspector = (MapObjectInspector) objectInspector;
            return hasType(mapInspector.getMapKeyObjectInspector(), types) ||
                    hasType(mapInspector.getMapValueObjectInspector(), types);
        }
        if (objectInspector instanceof StructObjectInspector) {
            for (org.apache.hadoop.hive.serde2.objectinspector.StructField field : ((StructObjectInspector) objectInspector).getAllStructFieldRefs()) {
                if (hasType(field.getFieldObjectInspector(), types)) {
                    return true;
                }
            }
            return false;
        }
        throw new IllegalArgumentException("Unknown object inspector type " + objectInspector);
    }

    private static DataType arrayType(DataType elementType) {
        return DataTypes.createArrayType(elementType);
    }

    private static DataType mapType(DataType keyType, DataType valueType) {
        return MapType.apply(keyType,valueType);
    }

    private static DataType rowType(DataType... fieldTypes) {
        StructField structField[] = new StructField[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++)
            structField[i] = DataTypes.createStructField("field_" + i, fieldTypes[i], true);
        return DataTypes.createStructType(structField);
    }

    public static int advance(Iterator<?> iterator, int numberToAdvance) {
        Preconditions.checkNotNull(iterator);
        Preconditions.checkArgument(numberToAdvance >= 0, "numberToAdvance must be nonnegative");

        int i;
        for(i = 0; i < numberToAdvance && iterator.hasNext(); ++i) {
            iterator.next();
        }

        return i;
    }


}
