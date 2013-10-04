package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.CallBufferFactory;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.RecordingCallBuffer;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.si.jmx.ManagedTransactor;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests to make sure that InsertOperation is inserting correctly.
 *
 * need to check
 *
 * Primary Keys
 * NonPrimary Keys
 *
 * @author Scott Fines
 * Created on: 10/4/13
 */
@RunWith(Parameterized.class)
public class InsertOperationTest {

    private static final KryoPool kryoPool = mock(KryoPool.class);
    private static final Snowflake snowflake = new Snowflake((short)1);
    private static final Random random = new Random(0l);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> data = Lists.newArrayList();


//        data.add(new Object[]{new TestingDataType[]{TestingDataType.TIMESTAMP},
//                new int[]{}
//        });
        for(TestingDataType dataType:TestingDataType.values()){
            data.add(new Object[]{new TestingDataType[]{dataType},
                    new int[]{1}
            });
        }

        for(TestingDataType dataType:TestingDataType.values()){
            for(TestingDataType secondType:TestingDataType.values()){
                int pkCol = random.nextInt(2)+1; //have to increment, cause that's how derby feeds it to Insert
                data.add(new Object[]{new TestingDataType[]{dataType,secondType},
                        new int[]{pkCol}
                });
            }
        }
        return data;
    }

    private final TestingDataType[] dataTypes;
    private final int[] primaryKeys;


    public InsertOperationTest(TestingDataType[] dataTypes, int[] primaryKeys) {
        this.dataTypes = dataTypes;
        this.primaryKeys = primaryKeys;
    }

    @Test
    public void testCanInsertDataNoPrimaryKeys() throws Exception {
        OperationInformation opInfo = mock(OperationInformation.class);
        when(opInfo.getUUIDGenerator()).thenReturn(snowflake.newGenerator(100));

        mockTransactions();

        DMLWriteInfo writeInfo = mock(DMLWriteInfo.class);
        when(writeInfo.getPkColumnMap()).thenReturn(null);
        when(writeInfo.getPkColumns()).thenReturn(null);

        final List<ExecRow> correctOutputRows = getInputRows();
        final List<ExecRow> rowsToWrite = Lists.newArrayList(correctOutputRows);

        final List<KVPair> output = Lists.newArrayListWithExpectedSize(rowsToWrite.size());
        mockOperationSink(writeInfo, output);

        SpliceOperation sourceOperation = mock(SpliceOperation.class);
        when(sourceOperation.nextRow()).thenAnswer(new Answer<ExecRow>() {
            @Override
            public ExecRow answer(InvocationOnMock invocation) throws Throwable {
                return rowsToWrite.size() > 0 ? rowsToWrite.remove(0) : null;
            }
        });
        when(sourceOperation.getExecRowDefinition()).thenReturn(TestingDataType.getTemplateOutput(dataTypes));

        InsertOperation operation = new InsertOperation(sourceOperation, opInfo,writeInfo,"10");
        operation.init(mock(SpliceOperationContext.class));

        NoPutResultSet resultSet = operation.executeScan();
        resultSet.open();

        Assert.assertEquals("Reports incorrect row count!",correctOutputRows.size(),resultSet.modifiedRowCount());
        Assert.assertEquals("Incorrect number of rows written!",correctOutputRows.size(),output.size());

        List<KVPair> correctOutput = getCorrectOutput(false,correctOutputRows);
        assertRowDataMatches(correctOutput,output);
    }


    @Test
    public void testCanInsertDataWithPrimaryKeys() throws Exception {
        OperationInformation opInfo = mock(OperationInformation.class);
        when(opInfo.getUUIDGenerator()).thenReturn(snowflake.newGenerator(100));

        mockTransactions();

        DMLWriteInfo writeInfo = mock(DMLWriteInfo.class);
        when(writeInfo.getPkColumnMap()).thenReturn(primaryKeys);
        when(writeInfo.getPkColumns()).thenAnswer(new Answer<FormatableBitSet>() {
            @Override
            public FormatableBitSet answer(InvocationOnMock invocation) throws Throwable {
                return DerbyDMLWriteInfo.fromIntArray(((DMLWriteInfo)invocation.getMock()).getPkColumnMap());
            }
        });

        final List<ExecRow> correctOutputRows = getInputRows();
        final List<ExecRow> rowsToWrite = Lists.newArrayList(correctOutputRows);

        final List<KVPair> output = Lists.newArrayListWithExpectedSize(rowsToWrite.size());
        mockOperationSink(writeInfo, output);

        SpliceOperation sourceOperation = mock(SpliceOperation.class);
        when(sourceOperation.nextRow()).thenAnswer(new Answer<ExecRow>() {
            @Override
            public ExecRow answer(InvocationOnMock invocation) throws Throwable {
                return rowsToWrite.size() > 0 ? rowsToWrite.remove(0) : null;
            }
        });
        when(sourceOperation.getExecRowDefinition()).thenReturn(TestingDataType.getTemplateOutput(dataTypes));

        InsertOperation operation = new InsertOperation(sourceOperation, opInfo,writeInfo,"10");
        operation.init(mock(SpliceOperationContext.class));

        NoPutResultSet resultSet = operation.executeScan();
        resultSet.open();

        Assert.assertEquals("Reports incorrect row count!",correctOutputRows.size(),resultSet.modifiedRowCount());
        Assert.assertEquals("Incorrect number of rows written!",correctOutputRows.size(),output.size());

        List<KVPair> correctOutput = getCorrectOutput(true,correctOutputRows);
        assertRowDataMatches(correctOutput,output);
    }

/*********************************************************************************************************************/
    /*private helper methods*/
    private void mockTransactions() throws IOException {
        ManagedTransactor mockTransactor = mock(ManagedTransactor.class);
        doNothing().when(mockTransactor).beginTransaction(any(Boolean.class));

        Transactor mockT = mock(Transactor.class);
        when(mockT.transactionIdFromString(any(String.class))).thenAnswer(new Answer<TransactionId>() {
            @Override
            public TransactionId answer(InvocationOnMock invocation) throws Throwable {
                return new TransactionId((String) invocation.getArguments()[0]);
            }
        });
        when(mockTransactor.getTransactor()).thenReturn(mockT);
        when(mockT.beginChildTransaction(any(TransactionId.class),any(Boolean.class))).thenAnswer(new Answer<TransactionId>() {
            @Override
            public TransactionId answer(InvocationOnMock invocation) throws Throwable {
                return (TransactionId) invocation.getArguments()[0];
            }
        });

        HTransactorFactory.setTransactor(mockTransactor);
    }

    private void assertRowDataMatches(List<KVPair> correctOutput,List<KVPair> output){
        for(int i=0;i<correctOutput.size();i++){
            KVPair correct = correctOutput.get(i);
            KVPair actual = output.get(i);

            Assert.assertArrayEquals("Incorrect data, for KVPair in position "+ i,correct.getValue(),actual.getValue());
        }
    }

    private List<KVPair> getCorrectOutput(final boolean usePrimaryKeys,List<ExecRow> rowsToWrite) {
        BitSet setCols = new BitSet(dataTypes.length);
        setCols.set(0,dataTypes.length);

        BitSet scalarCols = TestingDataType.getScalarFields(dataTypes);
        BitSet floatCols = TestingDataType.getFloatFields(dataTypes);
        BitSet doubleCols = TestingDataType.getDoubleFields(dataTypes);
        BitIndex index = BitIndexing.getBestIndex(setCols, scalarCols,floatCols,doubleCols);
        final EntryEncoder encoder = EntryEncoder.create(kryoPool,index);

        int[] pks = null;
        if(usePrimaryKeys){
            pks = new int[primaryKeys.length];
            for(int i=0;i<primaryKeys.length;i++){
                pks[i] = primaryKeys[i]-1;
            }
        }
        final int[] pksToUse = pks;
        List<KVPair> kvPairs = Lists.newArrayList(Lists.transform(rowsToWrite,new Function<ExecRow, KVPair>() {
            @Override
            public KVPair apply(@Nullable ExecRow input) {
                MultiFieldEncoder fieldEncoder = encoder.getEntryEncoder();
                fieldEncoder.reset();

                try {
                    RowMarshaller.sparsePacked().encodeRow(input.getRowArray(),null, fieldEncoder);
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
                byte[] dataBytes;
                try {
                    dataBytes = encoder.encode();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if(!usePrimaryKeys)
                    return new KVPair(snowflake.nextUUIDBytes(),dataBytes);
                else{
                    //need to generate the primary keys
                    try {
                        fieldEncoder.reset();
                        KeyType.BARE.encodeKey(input.getRowArray(), pksToUse, null, null, fieldEncoder);
                        return new KVPair(fieldEncoder.build(),dataBytes);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }

            }
        }));

        return kvPairs;
    }

    private List<ExecRow> getInputRows() throws StandardException {
        ExecRow template = TestingDataType.getTemplateOutput(dataTypes);
        Random random  = new Random(0l);
        List<ExecRow> rows = Lists.newArrayListWithCapacity(10);
        for(int i=0;i<10;i++){
            ExecRow nextRow = template.getNewNullRow();
            for(int pos=1;pos<=nextRow.nColumns();pos++){
                TestingDataType tdt = dataTypes[pos-1];
                tdt.setNext(nextRow.getColumn(pos),tdt.newObject(random));
            }
            rows.add(nextRow);
        }
        return rows;
    }

    private void mockOperationSink(DMLWriteInfo writeInfo, final List<KVPair> output) throws Exception {
        RecordingCallBuffer<KVPair> outputBuffer = mock(RecordingCallBuffer.class);
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                output.add((KVPair)invocation.getArguments()[0]);
                return null;
            }
        }).when(outputBuffer).add(any(KVPair.class));
        final CallBufferFactory<KVPair> bufferFactory = mock(CallBufferFactory.class);
        when(bufferFactory.writeBuffer(any(byte[].class),any(String.class))).thenReturn(outputBuffer);

        when(writeInfo.getOperationSink(any(SinkingOperation.class),any(byte[].class),any(String.class))).thenAnswer(new Answer<OperationSink>() {
            @Override
            public OperationSink answer(InvocationOnMock invocation) throws Throwable {
                SinkingOperation op = (SinkingOperation) invocation.getArguments()[0];
                byte[] taskId = (byte[])invocation.getArguments()[1];
                String txnId = (String)invocation.getArguments()[2];

                return new OperationSink(taskId,op,bufferFactory,txnId,snowflake.newGenerator(100));
            }
        });
    }



}
