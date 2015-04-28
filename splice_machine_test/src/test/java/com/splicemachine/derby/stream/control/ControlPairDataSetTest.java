package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.BaseStreamTest;
import com.splicemachine.derby.stream.PairDataSet;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import org.junit.*;
import java.util.Iterator;

/**
 * Created by jleach on 4/15/15.
 */
public class ControlPairDataSetTest extends BaseStreamTest {

    @Test
    public void testValues() {
        PairDataSet<ExecRow,ExecRow> pairDataSet = new ControlPairDataSet<ExecRow,ExecRow>(tenRows);
        Iterator<ExecRow> it = pairDataSet.values().toLocalIterator();
        int i = 0;
        while (it.hasNext()) {
            i++;
            Assert.assertEquals("grabbed the incorrect value from the map",10,it.next().nColumns());
        }
        Assert.assertEquals("grabbed the incorrect number of rows",10,i);
    }

    @Test
    public void testKeys() {
        PairDataSet<ExecRow,ExecRow> pairDataSet = new ControlPairDataSet<ExecRow,ExecRow>(tenRows);
        Iterator<ExecRow> it = pairDataSet.keys().toLocalIterator();
        int i = 0;
        while (it.hasNext()) {
            i++;
            Assert.assertEquals("grabbed the incorrect value from the map",1,it.next().nColumns());
        }
        Assert.assertEquals("grabbed the incorrect number of rows",10,i);
    }


    @Test
    public void testReduceByKey() throws StandardException {
        PairDataSet<ExecRow,ExecRow> pairDataSet = new ControlPairDataSet<ExecRow,ExecRow>(tenRows);
        PairDataSet<ExecRow,ExecRow> transformedDS = pairDataSet.reduceByKey(new SpliceFunction2<SpliceOperation,ExecRow, ExecRow, ExecRow>() {
            @Override
            public ExecRow call(ExecRow execRow, ExecRow execRow2) throws Exception {
                if (execRow == null || execRow2 == null)
                    return getExecRow(1,1);
                ExecRow row = getExecRow(1,1);
                row.getColumn(1).setValue(execRow.getColumn(1).getInt()+1);
                return row;
            }
        });
        Assert.assertEquals("records not reduced",2,transformedDS.keys().collect().size());
        Assert.assertEquals("records not reduced",2,transformedDS.values().collect().size());
        Iterator<ExecRow> it = transformedDS.values().toLocalIterator();
        while (it.hasNext()) {
            ExecRow er = it.next();
           Assert.assertEquals("value not 5",5,er.getColumn(1).getInt());
        }
    }

}
