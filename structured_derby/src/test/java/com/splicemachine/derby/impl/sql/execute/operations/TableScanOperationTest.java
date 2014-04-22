package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests to verify that TableScanOperation works correctly
 * (assuming certain serialization patterns, etc).
 *
 * For each data type, we need to confirm that the following
 * are properly deserialized:
 *
 * 1. Explicit, non-null value
 * 2. Explicit, null field
 * 3. missing field
 *
 * 3. is especially important, because the EntryPredicateFilter pattern does
 * not have sufficient information to populate all fields at read time; as a result,
 * it is possible to have null fields which are absent from the returned bytes.
 *
 * @author Scott Fines
 * Created on: 10/1/13
 */
@RunWith(Parameterized.class)
@Ignore
public class TableScanOperationTest {
    private static final KryoPool kryoPool = mock(KryoPool.class);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> dataTypes = Lists.newArrayList();

        dataTypes.add(new Object[]{new TestingDataType[]{TestingDataType.DECIMAL}});
        for(TestingDataType dataType: TestingDataType.values()){
            dataTypes.add(new Object[]{new TestingDataType[]{dataType}});
        }

        //do combinations of 2

//        dataTypes.add(new Object[]{new TestingDataType[]{TestingDataType.BOOLEAN,TestingDataType.VARCHAR}});
        for(TestingDataType dataType: TestingDataType.values()){
            for(TestingDataType secondPosType: TestingDataType.values()){
                dataTypes.add(new Object[]{new TestingDataType[]{dataType,secondPosType}});
            }
        }
//
//        for(TestingDataType dataType: TestingDataType.values()){
//            for(TestingDataType secondPosType: TestingDataType.values()){
//                for(TestingDataType thirdPosType: TestingDataType.values()){
//                    dataTypes.add(new Object[]{Arrays.asList(dataType,secondPosType,thirdPosType)});
//                }
//            }
//        }

        return dataTypes;

    }

    private final TestingDataType[] dataTypes;

    public TableScanOperationTest(TestingDataType[] dataTypes) {
        this.dataTypes = dataTypes;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCanProperlyScanTable() throws Exception {
        System.out.println(Arrays.toString(dataTypes));
        ExecRow testRow = getExecRow(dataTypes);
        int[] baseColumMap = new int[testRow.nColumns()];
        for(int i=0;i<baseColumMap.length;i++){
            baseColumMap[i] =i;
        }

        ScanInformation mockInfo = mock(ScanInformation.class);
        when(mockInfo.getConglomerateId()).thenReturn(1184l);
        when(mockInfo.getResultRow()).thenReturn(testRow);

        OperationInformation mockOpInfo = mock(OperationInformation.class);
        when(mockOpInfo.getBaseColumnMap()).thenReturn(baseColumMap);
        when(mockOpInfo.compactRow(any(ExecRow.class),any(FormatableBitSet.class),any(Boolean.class))).thenReturn(testRow);

        Pair<List<Cell>,List<ExecRow>> serializedRows = createRepresentativeRows(10,dataTypes);
        final List<Cell> actualRows = Lists.newArrayList(serializedRows.getFirst());

        MeasuredRegionScanner mockScanner = mock(MeasuredRegionScanner.class);
        //noinspection unchecked
        Answer<Boolean> answer = new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                if (actualRows.size() <= 0)
                    return false;

                @SuppressWarnings("unchecked") List<Cell> destList = (List<Cell>) invocation.getArguments()[0];
                destList.add(actualRows.remove(0));

                return true;
            }
        };
        when(mockScanner.nextRaw(any(List.class))).thenAnswer(answer);
        when(mockScanner.next(any(List.class))).thenAnswer(answer);

        HRegion mockRegion = mock(HRegion.class);
        doNothing().when(mockRegion).startRegionOperation();
        doNothing().when(mockRegion).closeRegionOperation();

        SpliceOperationContext mockContext = mock(SpliceOperationContext.class);
        when(mockContext.getScanner(any(Boolean.class))).thenReturn(mockScanner);
        when(mockContext.getScanner()).thenReturn(mockScanner);
        when(mockContext.getRegion()).thenReturn(mockRegion);

        TableScanOperation tableScanOp = new TableScanOperation(mockInfo,mockOpInfo,1,1);
        tableScanOp.init(mockContext);

        List<ExecRow> deserializedRows = Lists.newArrayListWithExpectedSize(actualRows.size());
        SpliceRuntimeContext runtimeContext = new SpliceRuntimeContext();
        ExecRow nextRow;
        do{
            nextRow = tableScanOp.nextRow(runtimeContext);
            if(nextRow!=null)
                deserializedRows.add(nextRow.getClone());
        }while(nextRow!=null);

        //verify that the size of returned rows matches
        List<ExecRow> correctRows = serializedRows.getSecond();
        Assert.assertEquals("Incorrect number of rows scanned!",correctRows.size(),deserializedRows.size());

        Comparator<ExecRow> comparator = new Comparator<ExecRow>() {
            @Override
            public int compare(ExecRow o1, ExecRow o2) {
                if(o1==null){
                    if(o2==null) return 0;
                    return -1;
                }else if(o2==null)
                    return 1;

                DataValueDescriptor[] first = o1.getRowArray();
                DataValueDescriptor[] second = o2.getRowArray();

                for(int i=0;i<first.length;i++){
                    if(i>second.length){
                        return 1;
                    }

                    DataValueDescriptor firstDvd = first[i];
                    DataValueDescriptor secondDvd = second[i];
                    int compare;
                    try {
                        compare = firstDvd.compare(secondDvd);
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                    if(compare!=0)
                        return compare;
                }
                return 0;
            }
        };
        Collections.sort(correctRows,comparator);
        Collections.sort(deserializedRows,comparator);

        for(int i=0;i<correctRows.size();i++){
            ExecRow actual = deserializedRows.get(i);
            ExecRow correct = correctRows.get(i);

            Assert.assertArrayEquals("Incorrect deserialized row!", correct.getRowArray(), actual.getRowArray());
        }
    }

    private Pair<List<Cell>,List<ExecRow>> createRepresentativeRows(int size,TestingDataType... dataTypes) throws StandardException, IOException {
        List<Cell> retList = Lists.newArrayListWithCapacity(size);
        ExecRow template = getExecRow(dataTypes);
        List<ExecRow> correctRows = Lists.newArrayListWithCapacity(size);

        Random random = new Random(0l);
        BitSet scalarFields = TestingDataType.getScalarFields(dataTypes);
        BitSet floatFields = TestingDataType.getFloatFields(dataTypes);
        BitSet doubleFields = TestingDataType.getDoubleFields(dataTypes);
        BitSet nonNullFields = new BitSet(dataTypes.length);
        nonNullFields.set(0,dataTypes.length);
        EntryEncoder entryEncoder = EntryEncoder.create(kryoPool,dataTypes.length,nonNullFields,scalarFields,floatFields,doubleFields);
        for(int i=0;i<size;i++){
            int explicitNullPos = -1;
            if(i%5==0)
                explicitNullPos = random.nextInt(dataTypes.length)+1;
            int implicitNullPos = -1;
            if(i%3==0)
                implicitNullPos = random.nextInt(dataTypes.length)+1;

            BitSet newNonNullFields = (BitSet)nonNullFields.clone();
            if(implicitNullPos>-1)
                newNonNullFields.clear(implicitNullPos-1);
            entryEncoder.reset(newNonNullFields);
            MultiFieldEncoder encoder = entryEncoder.getEntryEncoder();
            encoder.reset();
            ExecRow row  = template.getNewNullRow();
            int pos=1;
            for(TestingDataType dataType:dataTypes){
								if(pos == implicitNullPos)
										continue;
                if(pos == explicitNullPos){
                    switch (dataType) {
                        case REAL:
                            encoder.encodeEmptyFloat();
                            break;
                        case DOUBLE:
                            encoder.encodeEmptyDouble();
                            break;
                        default:
                            encoder.encodeEmpty();
                    }
                }else{
                    Object o = dataType.newObject(random);
                    dataType.setNext(row.getColumn(pos), o);
                    dataType.encode(o, encoder);
                }
                pos++;
            }
            byte[] rowKey = new byte[8];
            byte[] data = entryEncoder.encode();
            random.nextBytes(rowKey);
            retList.add(new KeyValue(rowKey, SpliceConstants.DEFAULT_FAMILY_BYTES,
                    SpliceConstants.PACKED_COLUMN_BYTES,data));
            correctRows.add(row);
        }

        return Pair.newPair(retList,correctRows);
    }


    private ExecRow getExecRow(TestingDataType[] dataTypes) {
        ExecRow template = new ValueRow(dataTypes.length);
        for(int i=0;i<dataTypes.length;i++){
            template.setColumn(i+1,dataTypes[i].getDataValueDescriptor());
        }
        return template;
    }
}
