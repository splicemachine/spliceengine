package com.splicemachine.derby.impl.load;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.SaltedKeyMarshall;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.writer.CallBufferFactory;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.RecordingCallBuffer;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.*;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;
import java.sql.Date;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 * Created on: 9/30/13
 */
public class ImportTaskTest {

    @Test
    public void testCanImportDate() throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final List<String[]> lines = Lists.newArrayList();
        for(int i=0;i<10;i++){
            lines.add(new String[]{format.format(new Date(i))});
            if(i==5){
                //also throw in a null in the middle
                lines.add(new String[]{});
            }
            if(i==7){
                //throw in a space to make sure it's treated properly
                lines.add(new String[]{" "});
            }
        }
        final ExecRow template = new ValueRow(1);
        template.setColumn(1,new SQLDate());

        testImport(lines, template,new Function<KVPair, ExecRow>() {
            @Override
            public ExecRow apply(@Nullable KVPair input) {
                assert input != null; //unneeded, but it makes idea happy
                MultiFieldDecoder decoder = MultiFieldDecoder.wrap(input.getValue(), mock(KryoPool.class));
                decoder.skip();
                ExecRow row = template.getNewNullRow();
                if (decoder.nextIsNull()) {
                    try {
                        row.getColumn(1).setToNull();
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    long next = decoder.decodeNextLong();
                    try {
                        row.getColumn(1).setValue(new java.sql.Date(next));
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
                return row;
            }
        });
    }

    @Test
    public void testCanImportChar() throws Exception {
        final List<String[]> lines = Lists.newArrayList();
        for(int i=0;i<10;i++){
            lines.add(new String[]{Integer.toString(i)});
            if(i==5){
                //also throw in a null in the middle
                lines.add(new String[]{});
            }
            if(i==7){
                //throw in a space to make sure it's treated properly
                lines.add(new String[]{" "});
            }
        }
        final ExecRow template = new ValueRow(1);
        template.setColumn(1,new SQLChar());

        testImport(lines, template,new Function<KVPair, ExecRow>() {
            @Override
            public ExecRow apply(@Nullable KVPair input) {
                assert input != null; //unneeded, but it makes idea happy
                MultiFieldDecoder decoder = MultiFieldDecoder.wrap(input.getValue(), mock(KryoPool.class));
                decoder.skip();
                ExecRow row = template.getNewNullRow();
                if (decoder.nextIsNull()) {
                    try {
                        row.getColumn(1).setToNull();
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    String next = decoder.decodeNextString();
                    try {
                        row.getColumn(1).setValue(next);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
                return row;
            }
        });
    }

    @Test
    public void testCanImportVarchar() throws Exception {
        final List<String[]> lines = Lists.newArrayList();
        for(int i=0;i<10;i++){
            lines.add(new String[]{Integer.toString(i)});
            if(i==5){
                //also throw in a null in the middle
                lines.add(new String[]{});
            }
            if(i==7){
                //throw in a space to make sure it's treated properly
                lines.add(new String[]{" "});
            }
        }
        final ExecRow template = new ValueRow(1);
        template.setColumn(1,new SQLVarchar());

        testImport(lines, template,new Function<KVPair, ExecRow>() {
            @Override
            public ExecRow apply(@Nullable KVPair input) {
                assert input != null; //unneeded, but it makes idea happy
                MultiFieldDecoder decoder = MultiFieldDecoder.wrap(input.getValue(), mock(KryoPool.class));
                decoder.skip();
                ExecRow row = template.getNewNullRow();
                if (decoder.nextIsNull()) {
                    try {
                        row.getColumn(1).setToNull();
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    String next = decoder.decodeNextString();
                    try {
                        row.getColumn(1).setValue(next);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
                return row;
            }
        });
    }

    @Test
    public void testCanImportBoolean() throws Exception {
        final List<String[]> lines = Lists.newArrayList();
        for(int i=0;i<10;i++){
            lines.add(new String[]{Boolean.toString(i%2==0)});
            if(i==5){
                //also throw in a null integer in the middle
                lines.add(new String[]{});
            }
            if(i==7){
                //throw in a space to make sure it's treated as null
                lines.add(new String[]{" "});
            }
        }
        final ExecRow template = new ValueRow(1);
        template.setColumn(1,new SQLBoolean());

        testImport(lines, template,new Function<KVPair, ExecRow>() {
            @Override
            public ExecRow apply(@Nullable KVPair input) {
                assert input != null; //unneeded, but it makes idea happy
                MultiFieldDecoder decoder = MultiFieldDecoder.wrap(input.getValue(), mock(KryoPool.class));
                decoder.skip();
                ExecRow row = template.getNewNullRow();
                if (decoder.nextIsNull()) {
                    try {
                        row.getColumn(1).setToNull();
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    boolean next = decoder.decodeNextBoolean();
                    try {
                        row.getColumn(1).setValue(next);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
                return row;
            }
        });
    }

    @Test
    public void testCanImportSmallint() throws Exception {
        final List<String[]> lines = Lists.newArrayList();
        for(int i=0;i<10;i++){
            lines.add(new String[]{Short.toString((short)i)});
            if(i==5){
                //also throw in a null integer in the middle
                lines.add(new String[]{});
            }
            if(i==7){
                //throw in a space to make sure it's treated as null
                lines.add(new String[]{" "});
            }
        }
        final ExecRow template = new ValueRow(1);
        template.setColumn(1,new SQLSmallint());

        testImport(lines, template,new Function<KVPair, ExecRow>() {
            @Override
            public ExecRow apply(@Nullable KVPair input) {
                assert input != null; //unneeded, but it makes idea happy
                MultiFieldDecoder decoder = MultiFieldDecoder.wrap(input.getValue(), mock(KryoPool.class));
                decoder.skip();
                ExecRow row = template.getNewNullRow();
                if (decoder.nextIsNull()) {
                    try {
                        row.getColumn(1).setToNull();
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    short next = decoder.decodeNextShort();
                    try {
                        row.getColumn(1).setValue(next);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
                return row;
            }
        });
    }

    @Test
    public void testCanImportTinyint() throws Exception {
        final List<String[]> lines = Lists.newArrayList();
        for(int i=0;i<10;i++){
            lines.add(new String[]{Byte.toString((byte)i)});
            if(i==5){
                //also throw in a null integer in the middle
                lines.add(new String[]{});
            }
            if(i==7){
                //throw in a space to make sure it's treated as null
                lines.add(new String[]{" "});
            }
        }
        final ExecRow template = new ValueRow(1);
        template.setColumn(1,new SQLTinyint());

        testImport(lines, template,new Function<KVPair, ExecRow>() {
            @Override
            public ExecRow apply(@Nullable KVPair input) {
                assert input != null; //unneeded, but it makes idea happy
                MultiFieldDecoder decoder = MultiFieldDecoder.wrap(input.getValue(), mock(KryoPool.class));
                decoder.skip();
                ExecRow row = template.getNewNullRow();
                if (decoder.nextIsNull()) {
                    try {
                        row.getColumn(1).setToNull();
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    byte next = decoder.decodeNextByte();
                    try {
                        row.getColumn(1).setValue(next);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
                return row;
            }
        });
    }

    @Test
    public void testCanImportLong() throws Exception {
        final List<String[]> lines = Lists.newArrayList();
        for(int i=0;i<10;i++){
            lines.add(new String[]{Long.toString(i)});
            if(i==5){
                //also throw in a null integer in the middle
                lines.add(new String[]{});
            }
            if(i==7){
                //throw in a space to make sure it's treated as null
                lines.add(new String[]{" "});
            }
        }
        final ExecRow template = new ValueRow(1);
        template.setColumn(1,new SQLLongint());

        testImport(lines, template,new Function<KVPair, ExecRow>() {
            @Override
            public ExecRow apply(@Nullable KVPair input) {
                assert input != null; //unneeded, but it makes idea happy
                MultiFieldDecoder decoder = MultiFieldDecoder.wrap(input.getValue(), mock(KryoPool.class));
                decoder.skip();
                ExecRow row = template.getNewNullRow();
                if (decoder.nextIsNull()) {
                    try {
                        row.getColumn(1).setToNull();
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    long next = decoder.decodeNextLong();
                    try {
                        row.getColumn(1).setValue(next);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
                return row;
            }
        });
    }

    @Test
    public void testCanImportDouble() throws Exception {
        final List<String[]> lines = Lists.newArrayList();
        for(int i=0;i<10;i++){
            lines.add(new String[]{Double.toString(i)});
            if(i==5){
                //also throw in a null integer in the middle
                lines.add(new String[]{});
            }
            if(i==7){
                //throw in a space to make sure it's treated as null
                lines.add(new String[]{" "});
            }
        }
        final ExecRow template = new ValueRow(1);
        template.setColumn(1,new SQLDouble());

        testImport(lines, template,new Function<KVPair, ExecRow>() {
            @Override
            public ExecRow apply(@Nullable KVPair input) {
                assert input != null; //unneeded, but it makes idea happy
                MultiFieldDecoder decoder = MultiFieldDecoder.wrap(input.getValue(), mock(KryoPool.class));
                decoder.skip();
                ExecRow row = template.getNewNullRow();
                if (decoder.nextIsNullDouble()) {
                    try {
                        row.getColumn(1).setToNull();
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    double next = decoder.decodeNextDouble();
                    try {
                        row.getColumn(1).setValue(next);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
                return row;
            }
        });
    }

    @Test
    public void testCanImportFloats() throws Exception {
        final List<String[]> lines = Lists.newArrayList();
        for(int i=0;i<10;i++){
            lines.add(new String[]{Float.toString(i)});
            if(i==5){
                //also throw in a null integer in the middle
                lines.add(new String[]{});
            }
            if(i==7){
                //throw in a space to make sure it's treated as null
                lines.add(new String[]{" "});
            }
        }
        final ExecRow template = new ValueRow(1);
        template.setColumn(1,new SQLReal());

        testImport(lines, template,new Function<KVPair, ExecRow>() {
            @Override
            public ExecRow apply(@Nullable KVPair input) {
                assert input != null; //unneeded, but it makes idea happy
                MultiFieldDecoder decoder = MultiFieldDecoder.wrap(input.getValue(), mock(KryoPool.class));
                decoder.skip();
                ExecRow row = template.getNewNullRow();
                if (decoder.nextIsNullFloat()) {
                    try {
                        row.getColumn(1).setToNull();
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    float next = decoder.decodeNextFloat();
                    try {
                        row.getColumn(1).setValue(next);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
                return row;
            }
        });
    }

    @Test
    public void testCanImportIntegers() throws Exception {
        final List<String[]> lines = Lists.newArrayList();
        for(int i=0;i<10;i++){
            lines.add(new String[]{Integer.toString(i)});
            if(i==5){
                //also throw in a null integer in the middle
                lines.add(new String[]{});
            }
            if(i==7){
                //throw in a space to make sure it's treated as null
                lines.add(new String[]{" "});
            }
        }
        final ExecRow template = new ValueRow(1);
        template.setColumn(1,new SQLInteger());

        testImport(lines, template,new Function<KVPair, ExecRow>() {
            @Override
            public ExecRow apply(@Nullable KVPair input) {
                assert input != null; //unneeded, but it makes idea happy
                MultiFieldDecoder decoder = MultiFieldDecoder.wrap(input.getValue(), mock(KryoPool.class));
                decoder.skip();
                ExecRow row = template.getNewNullRow();
                if (decoder.nextIsNull()) {
                    try {
                        row.getColumn(1).setToNull();
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    int next = decoder.decodeNextInt();
                    try {
                        row.getColumn(1).setValue(next);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
                return row;
            }
        });
    }

    private void testImport(final List<String[]> lines, final ExecRow template,
                            Function<KVPair,ExecRow> importReverserParser) throws Exception {
        ImportReader reader = mock(ImportReader.class);
        when(reader.nextRow()).thenAnswer(new ListAnswer(lines));

        ColumnContext colCtx = new ColumnContext.Builder()
                .nullable(true)
                .columnType(Types.INTEGER)
                .columnNumber(0).build();
        final ImportContext ctx = new ImportContext.Builder()
                .addColumn(colCtx)
                .path("/testPath")
                .destinationTable(1184l)
                .colDelimiter(",")
                .transactionId("TEST_TXN")
                .build();

        final List<KVPair> importedRows = Lists.newArrayListWithCapacity(lines.size());
        @SuppressWarnings("unchecked") RecordingCallBuffer<KVPair> testingBuffer = mock(RecordingCallBuffer.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                importedRows.add((KVPair)invocation.getArguments()[0]);
                return null;
            }
        }).when(testingBuffer).add(any(KVPair.class));

        @SuppressWarnings("unchecked") CallBufferFactory<KVPair> fakeBufferFactory = mock(CallBufferFactory.class);
        when(fakeBufferFactory.writeBuffer(any(byte[].class),any(String.class))).thenReturn(testingBuffer);
        final Snowflake snowflake = new Snowflake((short)1);
        Importer importer = new ParallelImporter(ctx,
                template,
                "TEST_TXN",
                1,
                100,
                fakeBufferFactory){
						@Override
						protected Snowflake.Generator getRandomGenerator() {
								return snowflake.newGenerator(lines.size());
						}
        };

        ImportTask importTask = new ImportTask("TEST_JOB",ctx,reader,importer,1,"TEST_TXN");
        importTask.doExecute();

        /*
         * We need to check that what got imported is exactly what we expect--same number,
         * and each row can be parsed back into the correct ExecRow
         */
        Assert.assertEquals("Incorrect number of rows returned!", lines.size(), importedRows.size());

        /*
         * Convert each "imported" KVPair back into an ExecRow.
         * We know the file format is BitIndex | 0 | <integer>, so we'll just use that
         */
        List<ExecRow> importedExecRows = Lists.newArrayList(Lists.transform(importedRows,importReverserParser));

        /*
         * Use the RowParser to parse lines into a list of ExecRow entries
         */
        List<ExecRow> correctRows = getCorrectExecRows(lines, ctx, template);

        //sort both lists, then compare them
        Comparator<ExecRow> rowComparator = ImportTestUtils.columnComparator(1);

        Collections.sort(importedExecRows, rowComparator);
        Collections.sort(correctRows,rowComparator);

        for(int i=0;i<correctRows.size();i++){
            ExecRow correctRow = correctRows.get(i);
            ExecRow importedRow = importedExecRows.get(i);

            Assert.assertEquals("Incorrect row value!",correctRow.getColumn(1),importedRow.getColumn(1));
        }
    }



    private List<ExecRow> getCorrectExecRows(List<String[]> lines,
                                             final ImportContext ctx,
                                             ExecRow template) {
        final RowParser parser = new RowParser(template,null,null,null);
        return Lists.newArrayList(Lists.transform(lines, new Function<String[], ExecRow>() {
            @Override
            public ExecRow apply(@Nullable String[] input) {
                try {
                    return parser.process(input, ctx.getColumnInformation()).getClone();
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }
        }));
    }

    private static class ListAnswer implements Answer<String[]>{
        private final LinkedList<String[]> list;

        private ListAnswer(List<String[]> list) {
            //make a copy to prevent removing everything from the passed in list
            this.list = Lists.newLinkedList(list);
        }

        @Override
        public String[] answer(InvocationOnMock invocation) throws Throwable {
            if(list.size()>0)
                return list.removeFirst();
            return null;
        }
    }


}
