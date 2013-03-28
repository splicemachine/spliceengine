package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.SpliceExecutionFactory;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.J2SEDataValueFactory;
import org.apache.derby.impl.sql.GenericColumnDescriptor;
import org.apache.derby.impl.sql.GenericResultDescription;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.impl.sql.compile.ResultColumn;
import org.apache.derby.impl.sql.execute.AggregatorInfo;
import org.apache.derby.impl.sql.execute.AggregatorInfoList;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.Types;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 * Created on: 3/27/13
 */
public class GroupedAggregateOperationTest {
    DataValueFactory dataValueFactory = new J2SEDataValueFactory();

    @Test
    @Ignore
    public void testAggregatesProperly() throws Exception{
        int aggregateItem = 4;
        int orderingItem = 3;
        boolean isInSortedOrder = false;
        boolean isRollup = false;

        ExecutionFactory executionFactory = new SpliceExecutionFactory();

        Activation mockActivation  = mock(Activation.class);
        when(mockActivation.getDataValueFactory()).thenReturn(dataValueFactory);
        when(mockActivation.getExecutionFactory()).thenReturn(executionFactory);

        ExecRow e2Row = new ValueRow(4);
        e2Row.setColumn(1, dataValueFactory.getNullVarchar(null));
        e2Row.setColumn(2,dataValueFactory.getNullInteger(null));
        e2Row.setColumn(3,dataValueFactory.getNullInteger(null));
        e2Row.setColumn(4,dataValueFactory.getNullObject(null));


        GeneratedMethod mockMethod = mock(GeneratedMethod.class);
        when(mockMethod.invoke(any())).thenReturn(e2Row);
        GeneratedClass mockActClass = mock(GeneratedClass.class);
        when(mockActClass.getMethod("e2")).thenReturn(mockMethod);



        DataValueDescriptor nameDesc = dataValueFactory.getVarcharDataValue("sfines");
        DataValueDescriptor ageDesc = dataValueFactory.getNullInteger(null);
        ageDesc.setValue(27);

        ExecRow mockRow = mock(ExecRow.class);

        when(mockRow.getColumn(1)).thenReturn(nameDesc);
        when(mockRow.getColumn(2)).thenReturn(ageDesc);

        DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER);
        ResultColumnDescriptor countColDesc = mock(ResultColumnDescriptor.class);
        when(countColDesc.getType()).thenReturn(dtd);
        when(countColDesc.getName()).thenReturn("##aggregate expression");
        when(countColDesc.getSourceSchemaName()).thenReturn("APP");
        when(countColDesc.getSourceTableName()).thenReturn("T");
        when(countColDesc.getColumnPosition()).thenReturn(3);
        when(countColDesc.isAutoincrement()).thenReturn(false);
        when(countColDesc.updatableByCursor()).thenReturn(false);
        when(countColDesc.hasGenerationClause()).thenReturn(false);
        GenericResultDescription countDesc
                = new GenericResultDescription(new ResultColumnDescriptor[]{countColDesc},"SELECT");

        AggregatorInfoList allAggs = new AggregatorInfoList();
        AggregatorInfo countInfo = new AggregatorInfo("COUNT",
                "org.apache.derby.impl.sql.execution.CountAggregator",
                2,
                1,
                3,
                false,countDesc );
        allAggs.add(countInfo);

        IndexColumnOrder order = new IndexColumnOrder(0,true, false);
        FormatableArrayHolder orderHolder = new FormatableArrayHolder(new IndexColumnOrder[]{order});

        GenericStorablePreparedStatement gsps = mock(GenericStorablePreparedStatement.class);
        when(gsps.getSavedObject(aggregateItem)).thenReturn(allAggs);
        when(gsps.getSavedObject(orderingItem)).thenReturn(orderHolder);
        when(gsps.getActivationClass()).thenReturn(mockActClass);

        SpliceOperation generator = mock(SpliceOperation.class);
        when(generator.getNextRowCore()).thenReturn(mockRow);
        when(mockActivation.getPreparedStatement()).thenReturn(gsps);

        new GroupedAggregateOperation(generator,
                isInSortedOrder,
                aggregateItem,
                orderingItem,
                mockActivation,
                mockMethod,
                -1,
                1,
                0d,
                0d,
                isRollup);

    }
}
