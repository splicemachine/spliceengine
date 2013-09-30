package com.splicemachine.derby.impl.load;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.CallBufferFactory;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.RecordingCallBuffer;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.SQLInteger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.Types;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 * Created on: 9/30/13
 */
public class ImportTaskTest {


    @Test
    public void testCanImportIntegers() throws Exception {
        List<String[]> lines = Lists.newArrayList();
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
        ImportReader reader = mock(ImportReader.class);
        when(reader.nextRow()).thenAnswer(new ListAnswer(lines));

        ColumnContext colCtx = new ColumnContext.Builder()
                .nullable(true)
                .columnType(Types.INTEGER)
                .columnNumber(0).build();
        ImportContext ctx = new ImportContext.Builder()
                .addColumn(colCtx)
                .path("/testPath")
                .destinationTable(1184l)
                .colDelimiter(",")
                .transactionId("TEST_TXN")
                .build();

        final List<KVPair> importedRows = Lists.newArrayListWithCapacity(lines.size());
        RecordingCallBuffer<KVPair> testingBuffer = mock(RecordingCallBuffer.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                importedRows.add((KVPair)invocation.getArguments()[0]);
                return null;
            }
        }).when(testingBuffer).add(any(KVPair.class));

        CallBufferFactory<KVPair> fakeBufferFactory = mock(CallBufferFactory.class);
        when(fakeBufferFactory.writeBuffer(any(byte[].class),any(String.class))).thenReturn(testingBuffer);
        ExecRow template = new ValueRow(1);
        template.setColumn(1,new SQLInteger());
        Importer importer = new ParallelImporter(ctx,
                template,
                "TEST_TXN",
                1,
                100,
                fakeBufferFactory);

        ImportTask importTask = new ImportTask("TEST_JOB",ctx,reader,importer,1,"TEST_TXN");
        importTask.doExecute();

        /*
         * We need to check that what got imported is exactly what we expect--same number,
         * and each row can be parsed back into the correct ExecRow
         */
        Assert.assertEquals("Incorrect number of rows returned!",lines.size(),importedRows.size());
    }

    private static class ListAnswer implements Answer<String[]>{
        private final LinkedList<String[]> list;

        private ListAnswer(List<String[]> list) {
            //make a copy to prevent removing everything from the passed in list
            this.list = Lists.newLinkedList(list);
        }

        @Override
        public String[] answer(InvocationOnMock invocation) throws Throwable {
            return list.removeFirst();
        }
    }
}
