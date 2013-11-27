package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.IndexValueRow;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.utils.Snowflake;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.UserType;
import org.apache.derby.impl.sql.execute.AggregatorInfo;
import org.apache.derby.impl.sql.execute.CountAggregator;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 *         Created on: 11/4/13
 */
public class GroupedAggregateOperationTest {

    @Test
    public void testCreatesRowsWithProperHashes() throws Exception {
        /*
         * We want to test that the rows that are emitted from GroupedAggregateOperation are
         * of the form
         *
         * <bucket> 0x00 <fixed uuid> 0x00 <known hash keys> 0x00 <uuid> 0x00 <known task id>
         */
        ExecRow sourceRowTemplate = new ValueRow(4);
        sourceRowTemplate.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(),
                new SQLInteger(),
                new SQLInteger(),
                new UserType()
        });
        final List<ExecRow> sourceRows = Lists.newArrayListWithCapacity(10);
        for(int i=0;i<20;i++){
            sourceRowTemplate.getColumn(1).setValue(i);
            sourceRows.add(sourceRowTemplate.getClone());
        }

        SpliceOperation mockSourceOperation = mock(SpliceOperation.class);
        when(mockSourceOperation.nextRow(any(SpliceRuntimeContext.class))).thenAnswer(new Answer<ExecRow>() {
            @Override
            public ExecRow answer(InvocationOnMock invocation) throws Throwable {
                if(sourceRows.size()>0)
                    return sourceRows.remove(0);
                return null;
            }
        });

        OperationInformation mockOpInformation = mock(OperationInformation.class);
        final Snowflake snowflake = new Snowflake((short)1);
        when(mockOpInformation.getUUIDGenerator()).thenAnswer(new Answer<Snowflake.Generator>() {
            @Override
            public Snowflake.Generator answer(InvocationOnMock invocation) throws Throwable {
                return snowflake.newGenerator(100);
            }
        });


        SpliceGenericAggregator countAgg = getCountAggregator(4,2,3);
        AggregateContext mockAggregateContext = mock(AggregateContext.class);
        when(mockAggregateContext.getAggregators()).thenReturn(new SpliceGenericAggregator[]{countAgg});
        ExecIndexRow sortTemplateRow = new IndexValueRow(sourceRowTemplate);
        when(mockAggregateContext.getSourceIndexRow()).thenReturn(sortTemplateRow);
        when(mockAggregateContext.getSortTemplateRow()).thenReturn((ExecIndexRow) sortTemplateRow.getClone());
        when(mockAggregateContext.getDistinctAggregators()).thenReturn(new SpliceGenericAggregator[]{});
        when(mockAggregateContext.getNonDistinctAggregators()).thenReturn(new SpliceGenericAggregator[]{countAgg});

        GroupedAggregateContext mockGroupedContext = mock(GroupedAggregateContext.class);
        when(mockGroupedContext.getGroupingKeys()).thenReturn(new int[]{0});
        when(mockGroupedContext.getGroupingKeyOrder()).thenReturn(new boolean[]{true});
        when(mockGroupedContext.getNonGroupedUniqueColumns()).thenReturn(new int[]{});

        GroupedAggregateOperation operation = new GroupedAggregateOperation(mockSourceOperation,
                mockOpInformation,mockAggregateContext,mockGroupedContext,false,false);
        SpliceOperationContext operationContext = mock(SpliceOperationContext.class);
        operation.open();
        operation.init(operationContext);

        SpliceRuntimeContext context = new SpliceRuntimeContext();
				context.setCurrentTaskId(snowflake.nextUUIDBytes());
        context.markAsSink();

        final List<KVPair> outputPairs = Lists.newArrayListWithExpectedSize(10);
        @SuppressWarnings("unchecked") CallBuffer<KVPair> mockBuffer = mock(CallBuffer.class);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                outputPairs.add((KVPair)invocation.getArguments()[0]);
                return null;
            }
        }).when(mockBuffer).add(any(KVPair.class));

				KeyEncoder keyEncoder = operation.getKeyEncoder(context);
				DataHash rowHash = operation.getRowHash(context);
				PairEncoder rowEncoder = new PairEncoder(keyEncoder,rowHash, KVPair.Type.INSERT);

        ExecRow nextSinkRow = operation.getNextSinkRow(context);
        Set<Integer> keysSeen = Sets.newHashSetWithExpectedSize(20);
        while(nextSinkRow!=null){
            int groupKeyValue = nextSinkRow.getColumn(1).getInt();
            Assert.assertFalse("Key already seen!",keysSeen.contains(groupKeyValue));
            keysSeen.add(groupKeyValue);

						mockBuffer.add(rowEncoder.encode(nextSinkRow));
            nextSinkRow = operation.getNextSinkRow(context);
        }
        Assert.assertEquals("Incorrect number of rows output!",20,outputPairs.size());

        //TODO -sf- assert KVPairs are correct in some way

    }

    private SpliceGenericAggregator getCountAggregator(int aggregatorColumnId, int inputColumn,int resultColumnId ) {
        CountAggregator execAggregator = new CountAggregator();
        execAggregator.setup("COUNT(*)");
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(execAggregator,aggregatorColumnId,inputColumn,resultColumnId);
        AggregatorInfo mockInfo = mock(AggregatorInfo.class);
        when(mockInfo.isDistinct()).thenReturn(false);
        aggregator.setAggInfo(mockInfo);
        return aggregator;
    }
}
