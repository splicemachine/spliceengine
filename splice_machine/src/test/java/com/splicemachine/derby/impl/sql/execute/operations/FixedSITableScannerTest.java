/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SIFilterFactory;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.filter.RowAccumulator;
import com.splicemachine.si.api.filter.SIFilter;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.filter.HRowAccumulator;
import com.splicemachine.si.testenv.ArchitectureSpecific;
import com.splicemachine.si.testenv.SITestDataEnv;
import com.splicemachine.si.testenv.SITestEnvironment;
import com.splicemachine.storage.*;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.uuid.Snowflake;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests specific scenarios around the SITableScanner (as opposed to the randomized testing types)
 *
 * @author Scott Fines
 *         Date: 4/9/14
 */
@Ignore
@Category(ArchitectureSpecific.class)
public class FixedSITableScannerTest{

    private OperationFactory opFactory;

    @Before
    public void setUp() throws Exception{
        SITestDataEnv testDataEnv =SITestEnvironment.loadTestDataEnvironment();
        opFactory = testDataEnv.getBaseOperationFactory();
    }

    @Test
    public void testScansBackSkipsSecondPrimaryKey() throws Exception{
        int[] keyDecodingMap=new int[]{1,-1};
        int[] keyColumnOrder=new int[]{1,0};
        int[] keyEncodingMap=new int[]{2,1};
        DataValueDescriptor[] data=new DataValueDescriptor[]{
                new SQLInteger(1),
//								new SQLDouble(Double.parseDouble("-8.98846567431158E307")), //encodes weirdly, so exercises our type checking
                new SQLReal(25f),
                new SQLVarchar("Hello")
        };
        int[] rowDecodingMap=new int[]{0,-1,-1,2};
        testScansProperly(keyDecodingMap,keyColumnOrder,null,keyEncodingMap,data,rowDecodingMap);
    }

    @Test
    public void testScansBackSkipsFirstPrimaryKey() throws Exception{
        int[] keyDecodingMap=new int[]{-1,1};
        int[] keyColumnOrder=new int[]{1,0};
        int[] keyEncodingMap=new int[]{2,1};
        DataValueDescriptor[] data=new DataValueDescriptor[]{
                new SQLInteger(1),
                new SQLDouble(Double.parseDouble("-8.98846567431158E307")), //encodes weirdly, so exercises our type checking
                new SQLVarchar("Hello")
        };
        int[] rowDecodingMap=new int[]{0,-1,-1,2};
        testScansProperly(keyDecodingMap,keyColumnOrder,null,keyEncodingMap,data,rowDecodingMap);
    }

    @Test
    public void testScansBackSkipsFirstPrimaryKeyDescendingAscending() throws Exception{
        int[] keyDecodingMap=new int[]{-1,1};
        int[] keyColumnOrder=new int[]{1,0};
        int[] keyEncodingMap=new int[]{2,1};
        DataValueDescriptor[] data=new DataValueDescriptor[]{
                new SQLInteger(1),
                new SQLDouble(Double.parseDouble("-8.98846567431158E307")), //encodes weirdly, so exercises our type checking
                new SQLVarchar("Hello")
        };
        int[] rowDecodingMap=new int[]{0,-1,-1,2};
        boolean[] keySortOrder=new boolean[]{false,true};
        testScansProperly(keyDecodingMap,keyColumnOrder,keySortOrder,keyEncodingMap,data,rowDecodingMap);
    }

    @Test
    public void testScansBackSkipsFirstPrimaryKeyAscendingDescending() throws Exception{
        int[] keyDecodingMap=new int[]{-1,1};
        int[] keyColumnOrder=new int[]{1,0};
        int[] keyEncodingMap=new int[]{2,1};
        DataValueDescriptor[] data=new DataValueDescriptor[]{
                new SQLInteger(1),
                new SQLDouble(Double.parseDouble("-8.98846567431158E307")), //encodes weirdly, so exercises our type checking
                new SQLVarchar("Hello")
        };
        int[] rowDecodingMap=new int[]{0,-1,-1,2};
        boolean[] keySortOrder=new boolean[]{true,false};
        testScansProperly(keyDecodingMap,keyColumnOrder,keySortOrder,keyEncodingMap,data,rowDecodingMap);
    }

    @Test
    public void testWorksWithTwoOutOfOrderPrimaryKeys() throws Exception{
        int[] keyDecodingMap=new int[]{2,1};
        int[] keyColumnOrder=new int[]{1,0};
        int[] keyEncodingMap=new int[]{2,1};
        testScansProperly(keyDecodingMap,keyColumnOrder,null,keyEncodingMap,null,null);
    }

    @Test
    public void testWorksWithTwoOutOfOrderPrimaryKeysDescendingAscending() throws Exception{
        int[] keyDecodingMap=new int[]{2,1};
        int[] keyColumnOrder=new int[]{1,0};
        int[] keyEncodingMap=new int[]{2,1};
        boolean[] keySortOrder=new boolean[]{false,true};
        testScansProperly(keyDecodingMap,keyColumnOrder,keySortOrder,keyEncodingMap,null,null);
    }

    @Test
    public void testWorksWithOneFloatKeyDescending() throws Exception{
                /*
				 * Test that the scanner properly decodes an entire row with a primary key
				 */
        int[] keyColumnPositionMap=new int[]{2};
        int[] keyColumnOrder=new int[]{0};
        boolean[] ascDescInfo=new boolean[]{false};
        testScansProperly(keyColumnPositionMap,keyColumnOrder,ascDescInfo,null,null,null);
    }

    @Test
    public void testWorksWithOneFloatKey() throws Exception{
				/*
				 * Test that the scanner properly decodes an entire row with a primary key
				 */
        int[] keyColumnPositionMap=new int[]{2};
        int[] keyColumnOrder=new int[]{0};
        testScansProperly(keyColumnPositionMap,keyColumnOrder);
    }

    @Test
    public void testWorksWithOneDoublePrimaryKey() throws Exception{
				/*
				 * Test that the scanner properly decodes an entire row with a primary key
				 */
        int[] keyColumnPositionMap=new int[]{1};
        int[] keyColumnOrder=new int[]{0};
        testScansProperly(keyColumnPositionMap,keyColumnOrder);
    }

    @Test
    public void testWorksWithNoPrimaryKeys() throws Exception{
				/*
				 * Test the situation where there are no primary keys. Hence, all data is stored in a row
				 */
        testScansProperly(null,null);
    }

    private static class MockFilter implements SIFilter{
        private RowAccumulator accumulator;

        private MockFilter(EntryAccumulator accumulator,
                           EntryDecoder decoder,
                           EntryPredicateFilter predicateFilter,
                           boolean isCountStar){
            this.accumulator=new HRowAccumulator(predicateFilter,decoder,accumulator,isCountStar);
        }

        @Override
        public void nextRow(){
        }

        @Override
        public RowAccumulator getAccumulator(){
            return accumulator;
        }

        @Override
        public DataFilter.ReturnCode filterCell(DataCell kv) throws IOException{
            if(kv.dataType()!=CellType.USER_DATA)
                return DataFilter.ReturnCode.SKIP;
            if(!accumulator.isFinished() && accumulator.isInteresting(kv)){
                if(!accumulator.accumulateCell(kv))
                    return DataFilter.ReturnCode.NEXT_ROW;
                return DataFilter.ReturnCode.INCLUDE;
            }else return DataFilter.ReturnCode.INCLUDE;
        }

    }

    protected void testScansProperly(int[] keyDecodingMap,int[] keyColumnOrder) throws StandardException, IOException{
        testScansProperly(keyDecodingMap,keyColumnOrder,null,null,null,null);

    }

    protected void testScansProperly(int[] keyDecodingMap,
                                     int[] keyColumnOrder,
                                     boolean[] keySortOrder,
                                     int[] keyEncodingMap,
                                     DataValueDescriptor[] correct,
                                     int[] rowDecodingMap) throws StandardException, IOException{
		/*
		 * Test that the scanner properly decodes an entire row with a primary key
		 */
        DataValueDescriptor[] data=new DataValueDescriptor[]{
                new SQLInteger(1),
                new SQLDouble(Double.parseDouble("-8.98846567431158E307")), //encodes weirdly, so exercises our type checking
                new SQLReal(25f),
                new SQLVarchar("Hello")
        };
        ExecRow row=new ValueRow(data.length);
        row.setRowArray(data);
        DescriptorSerializer[] serializers;
//				if(correct==null)
        serializers=VersionedSerializers.latestVersion(true).getSerializers(data);
//				else
//						serializers = VersionedSerializers.latestVersion(true).getSerializers(correct);
        byte[] key;
        int[] rowEncodingMap;
        int[] keyColumnTypes=null;
        if(keyColumnOrder!=null){
            if(keyEncodingMap==null){
                keyEncodingMap=new int[keyColumnOrder.length];
                for(int i=0;i<keyColumnOrder.length;i++){
                    keyEncodingMap[i]=keyDecodingMap[keyColumnOrder[i]];
                }
            }
            keyColumnTypes=new int[keyColumnOrder.length];
            for(int i=0;i<keyEncodingMap.length;i++){
                if(keyEncodingMap[i]<0) continue;
                keyColumnTypes[i]=data[keyEncodingMap[i]].getTypeFormatId();
            }
            rowEncodingMap=IntArrays.count(data.length);
            for(int pkCol : keyEncodingMap){
                rowEncodingMap[pkCol]=-1;
            }
            if(rowDecodingMap==null)
                rowDecodingMap=rowEncodingMap;

            KeyEncoder encoder=new KeyEncoder(NoOpPrefix.INSTANCE,BareKeyHash.encoder(keyEncodingMap,keySortOrder,serializers),NoOpPostfix.INSTANCE);
            key=encoder.getKey(row);
        }else{
            key=new Snowflake((short)1).nextUUIDBytes();
            rowEncodingMap=IntArrays.count(data.length);
            rowDecodingMap=rowEncodingMap;
        }

        EntryDataHash hash=new EntryDataHash(rowEncodingMap,null,serializers);
        hash.setRow(row);
        byte[] value=hash.encode();
        final DataCell dataKv=opFactory.newCell(key,SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,1l,value);
        final DataCell siKv=opFactory.newCell(key,SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,1l,SIConstants.EMPTY_BYTE_ARRAY);
        final boolean[] returned=new boolean[]{false};

        DataScanner scanner=mock(DataScanner.class);
        Answer<List<DataCell>> rowReturnAnswer=new Answer<List<DataCell>>(){

            @Override
            public List<DataCell> answer(InvocationOnMock invocation) throws Throwable{
                Assert.assertFalse("Attempted to call next() twice!",returned[0]);

                @SuppressWarnings("unchecked") List<DataCell> kvs=Arrays.asList(siKv,dataKv);
                returned[0]=true;
                return kvs;
            }
        };
        //noinspection unchecked
        when(scanner.next(anyInt())).thenAnswer(rowReturnAnswer);
//        when(scanner.internalNextRaw(any(List.class))).thenAnswer(rowReturnAnswer);
        //noinspection unchecked
//        when(scanner.next(any(List.class))).thenAnswer(rowReturnAnswer);

        DataScan scan=opFactory.newScan();
        ScanSetBuilder builder=new TableScannerBuilder(){
            @Override
            public DataSet buildDataSet() throws StandardException{
                throw new UnsupportedOperationException("improper access path for test");
            }
        }
                .scan(scan)
                .scanner(scanner)
                .tableVersion("2.0")
                .rowDecodingMap(rowDecodingMap);
        builder = ((TableScannerBuilder)builder)
                .filterFactory(
                        new SIFilterFactory(){
                            @Override
                            public SIFilter newFilter(EntryPredicateFilter predicateFilter,
                                                      EntryDecoder rowEntryDecoder,
                                                      EntryAccumulator accumulator,
                                                      boolean isCountStar) throws IOException{
                                return new MockFilter(accumulator,rowEntryDecoder,predicateFilter,isCountStar);
                            }
                        });

        if(correct!=null){
            ExecRow returnedRow=new ValueRow(correct.length);
            returnedRow.setRowArray(correct);
            builder=builder.template(returnedRow.getNewNullRow());
        }else
            builder=builder.template(row.getNewNullRow());

        if(keyColumnOrder!=null){
            FormatableBitSet accessedKeyCols=new FormatableBitSet(2);
            for(int i=0;i<keyColumnOrder.length;i++){
                if(keyDecodingMap[i]>=0)
                    accessedKeyCols.set(i);
            }
            builder=builder
                    .keyColumnEncodingOrder(keyColumnOrder)
                    .keyColumnTypes(keyColumnTypes)
                    .keyColumnSortOrder(keySortOrder)
                    .keyDecodingMap(keyDecodingMap)
                    .accessedKeyColumns(accessedKeyCols);
        }
        SITableScanner tableScanner=((TableScannerBuilder)builder).build();
        Assert.assertNotNull("Missing table scanner!",tableScanner);

        try{
            ExecRow next=tableScanner.next();
            if(correct==null)
                correct=row.getRowArray();
            Assert.assertArrayEquals("Incorrect scan decoding!",correct,next.getRowArray());
        }catch(Throwable t ){
            t.printStackTrace();
            throw t;
        }
    }
}
