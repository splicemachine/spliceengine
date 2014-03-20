package com.splicemachine.hbase;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.derby.utils.SpliceUtils;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * @author Scott Fines
 *         Created on: 9/25/13
 */
public class MockRegion {

    public static Answer<OperationStatus[]> getSuccessOnlyAnswer(final ObjectArrayList<Mutation> successfulPuts) {
        return new Answer<OperationStatus[]>() {
            @Override
            public OperationStatus[] answer(InvocationOnMock invocation) throws Throwable {
								Mutation[] writes = (Mutation[])invocation.getArguments()[0];
//                @SuppressWarnings("unchecked") Pair<Mutation, Integer>[] writes = (Pair<Mutation, Integer>[]) invocation.getArguments()[0];
                OperationStatus[] answer = new OperationStatus[writes.length];
                int i = 0;
                for (Mutation write: writes) {
                    successfulPuts.add(write);
                    answer[i] = new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
                    i++;
                }

                return answer;
            }
        };
    }

    public static Answer<OperationStatus[]> getNotServingRegionAnswer(){
        return new Answer<OperationStatus[]>() {
            @Override
            public OperationStatus[] answer(InvocationOnMock invocation) throws Throwable {
                throw new NotServingRegionException();
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static HRegion getMockRegion(Answer<OperationStatus[]> putAnswer) throws IOException {
        HRegionInfo testRegionInfo = mock(HRegionInfo.class);
        when(testRegionInfo.getStartKey()).thenReturn(HConstants.EMPTY_START_ROW);
        when(testRegionInfo.getEndKey()).thenReturn(HConstants.EMPTY_END_ROW);

        HTableDescriptor descriptor = mock(HTableDescriptor.class);
        when(descriptor.getNameAsString()).thenReturn("SPLICE_TEST");
        
        TableName tableName = SpliceUtils.getTableName("SPLICE_TEST");
        
        when(descriptor.getTableName()).thenReturn(tableName);
        
        HRegion testRegion = mock(HRegion.class);
        when(testRegion.getRegionInfo()).thenReturn(testRegionInfo);
        when(testRegion.getTableDesc()).thenReturn(descriptor);
        when(testRegion.batchMutate(new Put[]{any(Put.class)})).then(putAnswer);

        return testRegion;
    }
}
