package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.BooleanDataValue;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.*;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import com.splicemachine.derby.utils.test.TestingDataType;
import org.junit.*;
import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Created by jleach on 4/15/15.
 */
public class ControlDataSetTest extends BaseStreamTest{

    @Test
    public void testDistinct() {
        DataSet<SpliceOperation,ExecRow> control = new ControlDataSet<>(tenRowsTwoDuplicateRecords);
        Assert.assertEquals("Duplicate Rows Not Filtered", 10, control.distinct().collect().size());
    }

    @Test
    public void testMap() throws StandardException {
        DataSet<SpliceOperation,ExecRow> control = new ControlDataSet<>(tenRowsTwoDuplicateRecords);
        DataSet<SpliceOperation,ExecRow> mapppedDataSet = control.map(new SpliceFunction<SpliceOperation,ExecRow,ExecRow>() {
            @Override
            public ExecRow call(ExecRow execRow) throws Exception {
                ExecRow clone = execRow.getClone();
                clone.setColumn(1,TestingDataType.BOOLEAN.getDataValueDescriptor());
                return clone;
            }
        });
        Iterator<ExecRow> it = mapppedDataSet.toLocalIterator();
        while (it.hasNext())
            Assert.assertTrue("transform did not replace value",it.next().getColumn(1) instanceof BooleanDataValue);
    }

    @Test
    public void testLocalIterator() {
        int numElements = 0;
        DataSet<SpliceOperation,ExecRow> control = new ControlDataSet<>(tenRowsTwoDuplicateRecords);
        Iterator<ExecRow> it = control.toLocalIterator();
        while (it.hasNext() && it.next() !=null)
            numElements++;
        Assert.assertEquals("iterator loses records",tenRowsTwoDuplicateRecords.size(),numElements);
    }

    @Test
    public void testFold() throws StandardException {
        DataSet<SpliceOperation,ExecRow> control = new ControlDataSet<>(tenRowsTwoDuplicateRecords);
        ExecRow execRow = control.fold(null, new SpliceFunction2<SpliceOperation,ExecRow,ExecRow,ExecRow>() {
            @Override
            public ExecRow call(ExecRow execRow, ExecRow execRow2) throws Exception {
                if (execRow==null)
                    return execRow2;
                if (execRow2 ==null)
                    return execRow;
                ExecRow returnRow = execRow.getClone();
                returnRow.getColumn(1).setValue(execRow.getColumn(1).getInt()+execRow2.getColumn(1).getInt());
                return returnRow;
            }
        });
        Assert.assertEquals("summation did not work on execRow",47,execRow.getColumn(1).getInt());
    }

    @Test
    public void testIndex() throws Exception {
        DataSet<SpliceOperation,ExecRow> control = new ControlDataSet<>(tenRowsTwoDuplicateRecords);
        PairDataSet<SpliceOperation,ExecRow,ExecRow> pairDataSet = control.index(new SplicePairFunction<SpliceOperation,ExecRow, ExecRow>() {
            @Override
            public ExecRow internalCall(@Nullable ExecRow execRow) {
                try {
                    ExecRow clone = execRow.getClone();
                    DataValueDescriptor dvd = TestingDataType.INTEGER.getDataValueDescriptor();
                    dvd.setValue(1);
                    clone.setColumn(1, dvd);
                    return clone;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        Iterator<ExecRow> it = pairDataSet.keys().toLocalIterator();
        int i = 0;
        while (it.hasNext()) {
            i++;
            ExecRow execRow = it.next();
            Assert.assertEquals("Key not set",1,execRow.getColumn(1).getInt());
        }
        Assert.assertEquals("Number of rows incorrect",11,i);
    }

}