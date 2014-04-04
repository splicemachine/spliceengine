package com.splicemachine.derby.impl.sql.execute.operations;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.IndexRow;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * @author Scott Fines
 * Created on: 10/4/13
 */
@RunWith(Parameterized.class)
public class IndexRowReaderTest {

    private static final KryoPool kryoPool = mock(KryoPool.class);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> data = Lists.newArrayList();

        BitSet indexCols = new BitSet();
        indexCols.set(0);
        for(TestingDataType firstType:TestingDataType.values()){
            for(TestingDataType secondType:TestingDataType.values()){
                data.add(new Object[]{
                        new TestingDataType[]{firstType,secondType},
                        indexCols
                });
            }
        }

        return data;
    }

    private final TestingDataType[] outputDataTypes;
    private final BitSet indexedColumns;
    private final int[] baseColumnMap;
    private final int[] indexCols;

    public IndexRowReaderTest(TestingDataType[] outputDataTypes, BitSet indexedColumns) {
        this.outputDataTypes = outputDataTypes;
        this.indexedColumns = indexedColumns;

        this.baseColumnMap = new int[outputDataTypes.length];
        this.indexCols = new int[(int)indexedColumns.cardinality()];
        Arrays.fill(indexCols,-1);
        for(int i=0,pos=0;i<outputDataTypes.length;i++){
            if(!indexedColumns.get(i)){
                baseColumnMap[i] = i;
            }else{
                indexCols[pos] = i;
                pos++;
            }
        }

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDoesCorrectLookup() throws Exception {
        ExecutorService mockService = mock(ExecutorService.class);
        when(mockService.submit(any(Callable.class))).thenAnswer(new Answer<Future<List<Pair<IndexRowReader.RowAndLocation,Result>>>>() {
            @Override
            public Future<List<Pair<IndexRowReader.RowAndLocation,Result>>> answer(InvocationOnMock invocation) throws Throwable {
                Callable<List<Pair<IndexRowReader.RowAndLocation,Result>>> callable = (Callable<List<Pair<IndexRowReader.RowAndLocation, Result>>>) invocation.getArguments()[0];
                Future<List<Pair<IndexRowReader.RowAndLocation,Result>>> future = mock(Future.class);
                try{
                    List<Pair<IndexRowReader.RowAndLocation, Result>> call = callable.call();
                    when(future.get()).thenReturn(call);
                }catch(Exception e){
                    when(future.get()).thenThrow(new ExecutionException(e));
                }
                return future;
            }
        });

        ExecRow templateOutput = TestingDataType.getTemplateOutput(outputDataTypes);
        Map<byte[],ExecRow> outputRows = getCorrectOutput();

        final List<ExecRow> inputIndexRows = getInputIndexRows(outputRows);
        final Map<byte[],Result> inputHeapRows = getInputHeapRows(outputRows);
        List<IndexRowReader.RowAndLocation> correctOutput = formatOutput(outputRows);

        HTableInterface table = mock(HTableInterface.class);
        when(table.get(any(List.class))).thenAnswer(new Answer<Result[]>() {
            @Override
            public Result[] answer(InvocationOnMock invocation) throws Throwable {
                List<Get> gets = (List<Get>) invocation.getArguments()[0];
                Result[] results = new Result[gets.size()];
                for(int i=0;i<results.length;i++){
                    results[i] = inputHeapRows.get(gets.get(i).getRow());
                }
                return results;
            }
        });

        SpliceOperation mockSource = mock(SpliceOperation.class);
        when(mockSource.nextRow(any(SpliceRuntimeContext.class))).thenAnswer(new Answer<ExecRow>() {
            @Override
            public ExecRow answer(InvocationOnMock invocation) throws Throwable {
                return inputIndexRows.size() > 0 ? inputIndexRows.remove(0) : null;
            }
        });

        BitSet heapCols = new BitSet(outputDataTypes.length);
        heapCols.set(0,outputDataTypes.length);
        for(int i=indexedColumns.nextSetBit(0);i>=0;i=indexedColumns.nextSetBit(i+1)){
            heapCols.clear(i);
        }
				int[] adjustedBaseColumMap = new int[(int)heapCols.length()];
				Arrays.fill(adjustedBaseColumMap,-1);
				for(int i=heapCols.nextSetBit(0),pos=0;i>=0;i=heapCols.nextSetBit(i+1),pos++){
						adjustedBaseColumMap[pos] = i;
				}

        EntryPredicateFilter epf = new EntryPredicateFilter(heapCols, new ObjectArrayList<Predicate>());
        byte[] epfBytes = epf.toBytes();
        int[] typeIds = new int[]{80, 80};
        IndexRowReader rowReader = new IndexRowReader(mockService,table,mockSource,
                1,1,templateOutput,"10.IRO",indexCols,1184l,adjustedBaseColumMap,
								epfBytes,new SpliceRuntimeContext(new TempTable(SpliceConstants.TEMP_TABLE_BYTES),kryoPool),
                null);

        List<IndexRowReader.RowAndLocation> actualOutput = Lists.newArrayListWithExpectedSize(correctOutput.size());
        IndexRowReader.RowAndLocation rowAndLocation;
        do{
            rowAndLocation = rowReader.next();
            if(rowAndLocation!=null)
                actualOutput.add(rowAndLocation);
        }while(rowAndLocation!=null);

        Assert.assertArrayEquals("Incorrect output!", correctOutput.toArray(), actualOutput.toArray());
    }

    private List<IndexRowReader.RowAndLocation> formatOutput(Map<byte[], ExecRow> outputRows) {
        List<IndexRowReader.RowAndLocation> output = Lists.newArrayListWithCapacity(outputRows.size());
        for(byte[] outputRowKey:outputRows.keySet()){
            ExecRow outputRow = outputRows.get(outputRowKey);

            IndexRowReader.RowAndLocation rowAndLocation = new IndexRowReader.RowAndLocation();
            rowAndLocation.row = outputRow;
            rowAndLocation.rowLocation = outputRowKey;
            output.add(rowAndLocation);
        }
        return output;
    }

    private Map<byte[],ExecRow> getCorrectOutput() throws StandardException {
        Map<byte[],ExecRow> output = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        ExecRow template = TestingDataType.getTemplateOutput(outputDataTypes);

        Snowflake snowflake = new Snowflake((short)1);
        Random random = new Random(0l);
        for(int i=0;i<10;i++){
            ExecRow nextOutputRow = template.getNewNullRow();
            int pos=1;
            for(TestingDataType tdt:outputDataTypes){
                tdt.setNext(nextOutputRow.getColumn(pos),tdt.newObject(random));
                pos++;
            }

            byte[] rowKey = snowflake.nextUUIDBytes();
            output.put(rowKey,nextOutputRow);
        }

        return output;
    }
    private Map<byte[], Result> getInputHeapRows(Map<byte[],ExecRow> outputRows) throws StandardException, IOException {
        Map<byte[],Result> results = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        BitSet setCols = new BitSet(outputDataTypes.length);
        setCols.set(0,outputDataTypes.length);
        setCols.andNot(indexedColumns);

        BitSet scalarFields = TestingDataType.getScalarFields(outputDataTypes);
        scalarFields.and(setCols);
        BitSet floatFields = TestingDataType.getFloatFields(outputDataTypes);
        floatFields.and(setCols);
        BitSet doubleFields = TestingDataType.getDoubleFields(outputDataTypes);
        doubleFields.and(setCols);

        BitIndex index = BitIndexing.getBestIndex(setCols,scalarFields,floatFields,doubleFields);
        EntryEncoder encoder = EntryEncoder.create(kryoPool, index);
        for(byte[] outputRowKey:outputRows.keySet()){
            encoder.getEntryEncoder().reset();
            ExecRow outputRow = outputRows.get(outputRowKey);

            int [] heapCols = new int[outputDataTypes.length-indexCols.length];
            for(int i=0,pos=0;i<outputDataTypes.length;i++){
                if(!indexedColumns.get(i)){
                    heapCols[pos] = i;
                    pos++;
                }
            }
						DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(true).getSerializers(outputRow);
						EntryDataHash rowHash = new EntryDataHash(heapCols,null,serializers);
						rowHash.setRow(outputRow);
						byte[] outputBytes = rowHash.encode();
//            RowMarshaller.sparsePacked().encodeRow(outputRow.getRowArray(), heapCols, encoder.getEntryEncoder());
//            byte[] outputBytes = encoder.encode();

            KeyValue kv = new KeyValue(outputRowKey,SpliceConstants.DEFAULT_FAMILY_BYTES,
										SpliceConstants.PACKED_COLUMN_BYTES,outputBytes);
            Result result = new Result(new KeyValue[]{kv});
            results.put(outputRowKey,result);
        }

        return results;
    }

    private List<ExecRow> getInputIndexRows(Map<byte[],ExecRow> outputRows) throws StandardException {
        ExecRow indexTemplate = getIndexTemplate();

        List<ExecRow> indexRows = Lists.newArrayList();
        for(byte[] outputRowKey:outputRows.keySet()){
            ExecRow nextIndexRow = indexTemplate.getNewNullRow();
            ExecRow outputRow = outputRows.get(outputRowKey);
            for(int i=0;i<indexCols.length;i++){
                int outputPos = indexCols[i];
                nextIndexRow.setColumn(i+1,outputRow.getColumn(outputPos+1).cloneValue(true));
            }
            nextIndexRow.getColumn(nextIndexRow.nColumns()).setValue(outputRowKey);
            indexRows.add(nextIndexRow);
        }

        return indexRows;
    }

    private ExecRow getIndexTemplate() {
        ExecRow templateInput = new IndexRow((int)indexedColumns.cardinality()+1);
        for(int i=0;i<indexCols.length;i++){
            int position = indexCols[i];
            TestingDataType indexType = outputDataTypes[position];
            templateInput.setColumn(i+1,indexType.getDataValueDescriptor());
        }
        templateInput.setColumn(templateInput.nColumns(),new HBaseRowLocation());
        return templateInput;
    }

}
