package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.CallBufferFactory;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.RecordingCallBuffer;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobResults;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.SimpleJobResults;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.si.jmx.ManagedTransactor;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.kryo.KryoPool;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

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
 * These tests work by running a fixed series of ExecRows through the InsertOperation, and collecting
 * the output byte[]. Then the correct byte arrays are generated, and compared against what was output. If
 * they match, the InsertOperation works (as long as the write pipeline works). If they do not, then the
 * Insert operation is failing.
 *
 * @author Scott Fines
 * Created on: 10/4/13
 */
@RunWith(Parameterized.class)
public class InsertOperationTest {

		private static final TempTable table = new TempTable(SpliceConstants.TEMP_TABLE_BYTES);
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
        final List<ExecRow> correctOutputRows = getInputRows();
        final List<KVPair> output = doInsertionOperation(false,correctOutputRows);

        List<KVPair> correctOutput = getCorrectOutput(false,correctOutputRows);
        assertRowDataMatches(correctOutput,output);
    }

    @Test
    public void testCanInsertDataWithPrimaryKeys() throws Exception {
        final List<ExecRow> correctOutputRows = getInputRows();
        final List<KVPair> output = doInsertionOperation(true,correctOutputRows);

        List<KVPair> correctOutput = getCorrectOutput(true,correctOutputRows);
        assertRowDataMatches(correctOutput,output);
    }

    /*********************************************************************************************************************/
    /*private helper methods*/

    private List<KVPair> doInsertionOperation(boolean usePks,List<ExecRow> correctOutputRows) throws Exception {
        OperationInformation opInfo = mock(OperationInformation.class);
        when(opInfo.getUUIDGenerator()).thenReturn(snowflake.newGenerator(100));

        mockTransactions();

        DMLWriteInfo writeInfo = mock(DMLWriteInfo.class);
        when(writeInfo.getPkColumnMap()).thenReturn(usePks? primaryKeys:null);
        when(writeInfo.getPkColumns()).thenReturn(usePks? DerbyDMLWriteInfo.fromIntArray(primaryKeys):null);

        final List<ExecRow> rowsToWrite = Lists.newArrayList(correctOutputRows);

        final List<KVPair> output = Lists.newArrayListWithExpectedSize(rowsToWrite.size());

        SpliceObserverInstructions mockInstructions = mock(SpliceObserverInstructions.class);
        doNothing().when(mockInstructions).setTransactionId(any(String.class));

        when(writeInfo.buildInstructions(any(SpliceOperation.class))).thenReturn(mockInstructions);

        SpliceOperation sourceOperation = mock(SpliceOperation.class);
        when(sourceOperation.nextRow(any(SpliceRuntimeContext.class))).thenAnswer(new Answer<ExecRow>() {
            @Override
            public ExecRow answer(InvocationOnMock invocation) throws Throwable {
                return rowsToWrite.size() > 0 ? rowsToWrite.remove(0) : null;
            }
        });
        when(sourceOperation.getExecRowDefinition()).thenReturn(TestingDataType.getTemplateOutput(dataTypes));

        mockOperationSink(sourceOperation, output);

        InsertOperation operation = new InsertOperation(sourceOperation, opInfo,writeInfo,"10");
        operation.init(mock(SpliceOperationContext.class));
        when(mockInstructions.getTopOperation()).thenReturn(operation);

        NoPutResultSet resultSet = operation.executeScan(new SpliceRuntimeContext(table,kryoPool));
        resultSet.open();

        Assert.assertEquals("Reports incorrect row count!", correctOutputRows.size(), resultSet.modifiedRowCount());
        Assert.assertEquals("Incorrect number of rows written!",correctOutputRows.size(),output.size());
        return output;
    }

    @SuppressWarnings("unchecked")
    private void mockTransactions() throws IOException {
        ManagedTransactor mockTransactor = mock(ManagedTransactor.class);
        doNothing().when(mockTransactor).beginTransaction(any(Boolean.class));

        TransactionManager mockControl = mock(TransactionManager.class);
        when(mockControl.transactionIdFromString(any(String.class))).thenAnswer(new Answer<TransactionId>() {
						@Override
						public TransactionId answer(InvocationOnMock invocation) throws Throwable {
								return new TransactionId((String) invocation.getArguments()[0]);
						}
				});
				Transactor mockT = mock(Transactor.class);
        when(mockTransactor.getTransactor()).thenReturn(mockT);
        when(mockControl.beginChildTransaction(any(TransactionId.class),any(Boolean.class))).thenAnswer(new Answer<TransactionId>() {
						@Override
						public TransactionId answer(InvocationOnMock invocation) throws Throwable {
								return (TransactionId) invocation.getArguments()[0];
						}
				});

        HTransactorFactory.setTransactor(mockTransactor);
				HTransactorFactory.setTransactionManager(mockControl);
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

        return Lists.newArrayList(Lists.transform(rowsToWrite,new Function<ExecRow, KVPair>() {
            @Override
            public KVPair apply(@Nullable ExecRow input) {
                MultiFieldEncoder fieldEncoder = encoder.getEntryEncoder();
                fieldEncoder.reset();

                try {
                    //noinspection ConstantConditions
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
    }

    private List<ExecRow> getInputRows() throws StandardException {
        ExecRow template = TestingDataType.getTemplateOutput(dataTypes);
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

    @SuppressWarnings("unchecked")
    private void mockOperationSink(SpliceOperation rowSourceOp, final List<KVPair> output) throws Exception {
        RecordingCallBuffer<KVPair> outputBuffer = mock(RecordingCallBuffer.class);
        doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                output.add((KVPair)invocation.getArguments()[0]);
                return null;
            }
        }).when(outputBuffer).add(any(KVPair.class));
        final CallBufferFactory<KVPair> bufferFactory = mock(CallBufferFactory.class);
        when(bufferFactory.writeBuffer(any(byte[].class), any(String.class))).thenReturn(outputBuffer);
				when(bufferFactory.writeBuffer(any(byte[].class), any(String.class),any(MetricFactory.class))).thenReturn(outputBuffer);

        RowProvider mockProvider = mock(RowProvider.class);
        when(mockProvider.shuffleRows(any(SpliceObserverInstructions.class))).thenAnswer(new Answer<JobResults>(){

            @Override
            public JobResults answer(InvocationOnMock invocation) throws Throwable {
                SpliceObserverInstructions observerInstructions = (SpliceObserverInstructions) invocation.getArguments()[0];

                SpliceOperation op = observerInstructions.getTopOperation();

                OperationSink opSink = new OperationSink(Bytes.toBytes("TEST_TASK"),(DMLWriteOperation)op,bufferFactory,"TEST_TXN",-1l,0l);

                TaskStats sink = opSink.sink(Bytes.toBytes("1184"), new SpliceRuntimeContext(table,kryoPool));
                JobStats stats = mock(JobStats.class);
                when(stats.getTaskStats()).thenReturn(Arrays.asList(sink));

								JobFuture future = mock(JobFuture.class);
								doNothing().when(future).cleanup();

								return new SimpleJobResults(stats, future);
            }
        });

        when(rowSourceOp.getMapRowProvider(any(SpliceOperation.class),any(PairDecoder.class),any(SpliceRuntimeContext.class)))
                .thenReturn(mockProvider);
    }



}
