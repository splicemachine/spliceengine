package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.IndexValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.scalar.ScalarAggregateSource;
import com.splicemachine.derby.impl.sql.execute.operations.scalar.ScalarAggregator;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.UserType;
import org.apache.derby.impl.sql.execute.CountAggregator;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 * Created on: 10/8/13
 */
public class ScalarAggregatorTest {


    @Test
    public void testMergesCorrectly() throws Exception {
        ExecRow firstAggRow = getAggregateRow();
        ExecRow secondAggRow = getAggregateRow();

        final List<ExecRow> inputRows = Lists.newArrayList(firstAggRow, secondAggRow);
        CountAggregator countAggregator = new CountAggregator();
        SpliceGenericAggregator agg = new SpliceGenericAggregator(countAggregator,2,1,1);
        ScalarAggregateSource source = mock(ScalarAggregateSource.class);
        when(source.nextRow(any(SpliceRuntimeContext.class))).thenAnswer(new Answer<ExecRow>() {
            @Override
            public ExecRow answer(InvocationOnMock invocation) throws Throwable {
                if(inputRows.size()>0)
                    return inputRows.remove(0);
                return null;
            }
        });
        ScalarAggregator aggregator = new ScalarAggregator(source,new SpliceGenericAggregator[]{agg},true,false,false);

        ExecRow correct = aggregator.aggregate(new SpliceRuntimeContext());

        Assert.assertEquals("Incorrect merging process!",20,((CountAggregator)((UserType)correct.getColumn(2)).getObject()).getResult().getInt());
    }

    @Test
    public void testAggregatesCorrectly() throws Exception {
        ExecRow aggregatedRow = getAggregateRow();
        Object countObj = aggregatedRow.getColumn(2).getObject();
        Assert.assertTrue("Incorrect aggregator type!",countObj instanceof CountAggregator);
        CountAggregator agg = (CountAggregator)countObj;
        int count = agg.getResult().getInt();
        Assert.assertEquals("Incorrect count!",10,count);
    }

    @Test
    public void testAggregatesCorrectlyOneInputRow() throws Exception {
        ExecRow aggregatedRow = getAggregateOneInputRow();
        Object countObj = aggregatedRow.getColumn(2).getObject();
        Assert.assertTrue("Incorrect aggregator type!",countObj instanceof CountAggregator);
        CountAggregator agg = (CountAggregator)countObj;
        int count = agg.getResult().getInt();
        Assert.assertEquals("Incorrect count!",1,count);
    }

    private ExecRow getAggregateRow() throws StandardException, IOException {
        CountAggregator countAggregator = new CountAggregator();
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(countAggregator,2,1,1);

        ScalarAggregateSource source = mock(ScalarAggregateSource.class);
        final List<ExecRow> inputRows = getInputRows();
        when(source.nextRow(any(SpliceRuntimeContext.class))).thenAnswer(new Answer<ExecRow>() {
            @Override
            public ExecRow answer(InvocationOnMock invocation) throws Throwable {
                if(inputRows.size()>0)
                    return inputRows.remove(0);
                return null;
            }
        });

        ScalarAggregator aggregate = new ScalarAggregator(source,new SpliceGenericAggregator[]{aggregator},false,true,false);

        ExecRow aggregatedRow = aggregate.aggregate(new SpliceRuntimeContext());
        aggregate.finish(aggregatedRow);
        return aggregatedRow;
    }

    private ExecRow getAggregateOneInputRow() throws StandardException, IOException {
        CountAggregator countAggregator = new CountAggregator();
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(countAggregator,2,1,1);

        ScalarAggregateSource source = mock(ScalarAggregateSource.class);
        final List<ExecRow> inputRows = getInputRows();
        when(source.nextRow(any(SpliceRuntimeContext.class))).thenAnswer(new Answer<ExecRow>() {
            @Override
            public ExecRow answer(InvocationOnMock invocation) throws Throwable {
                if(inputRows.size()>0)
                    return inputRows.remove(0);
                return null;
            }
        }).thenThrow(new AssertionError("Expected to be called only once"));

        ScalarAggregator aggregate = new ScalarAggregator(source,new SpliceGenericAggregator[]{aggregator},false,true,true);

        ExecRow aggregatedRow = aggregate.aggregate(new SpliceRuntimeContext());
        aggregate.finish(aggregatedRow);
        return aggregatedRow;
    }

    private List<ExecRow> getInputRows() throws StandardException {
        ExecRow row = new IndexValueRow(new ValueRow(2));
        row.getRowArray()[0] = new SQLInteger();
        row.getRowArray()[1] = new UserType();
        List<ExecRow> rows = Lists.newArrayList();
        for(int i=0;i<10;i++){
            row.getColumn(1).setValue(i);
            ExecRow rowToAdd = row.getClone();
            rows.add(rowToAdd);
        }

        return rows;
    }
}
