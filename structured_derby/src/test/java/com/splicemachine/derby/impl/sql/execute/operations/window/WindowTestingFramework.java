package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.WindowFunction;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.derby.impl.services.reflect.SpliceReflectClasses;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;

/**
 * @author Jeff Cunningham
 *         Date: 8/18/14
 */
public class WindowTestingFramework {
    protected static LazyDataValueFactory dvf = new LazyDataValueFactory();
    protected static SpliceReflectClasses cf = new SpliceReflectClasses();

    @BeforeClass
    public static void startup() throws StandardException {
        Properties bootProps = new Properties();
        bootProps.setProperty("derby.service.jdbc", "org.apache.derby.jdbc.InternalDriver");
        bootProps.setProperty("derby.service.authentication", AuthenticationService.MODULE);

        Monitor.startMonitor(bootProps,System.out);
        cf.boot(false, new Properties());
        dvf.boot(false, new Properties());
    }

    @Test
    public void testDenseRank() throws StandardException {
        List<ExecRow> rows = new ArrayList<ExecRow>(1000);
        WindowFunction denseRank = new DenseRankFunction();

        denseRank.setup(cf, "dense_rank", DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false));
        for (int i=0;i<1000;i++) {
            ValueRow valueRow = new ValueRow(4);
            valueRow.setColumn(1, dvf.getDataValue(i, null));
            valueRow.setColumn(2, dvf.getDataValue((double)i, null));
            valueRow.setColumn(3, dvf.getDataValue(""+i, null));
            denseRank.accumulate(valueRow.getColumn(1), null);
            valueRow.setColumn(4, denseRank.getResult().cloneValue(true));
            rows.add(valueRow);
        }
        for (ExecRow row: rows) {
            System.out.println(row);
        }

    }

    @Test
    public void testRank() throws StandardException {
        List<ExecRow> rows = new ArrayList<ExecRow>(1000);
        WindowFunction rank = new RankFunction();

        rank.setup(cf, "rank", DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false));
        for (int i=0;i<1000;i++) {
            ValueRow valueRow = new ValueRow(4);
            valueRow.setColumn(1, dvf.getDataValue(i, null));
            valueRow.setColumn(2, dvf.getDataValue((double)i, null));
            valueRow.setColumn(3, dvf.getDataValue(""+i, null));
            rank.accumulate(valueRow.getColumn(1), null);
            valueRow.setColumn(4, rank.getResult().cloneValue(true));
            rows.add(valueRow);
        }
        for (ExecRow row: rows) {
            System.out.println(row);
        }

    }

    @Test
    public void testRowNumber() throws StandardException {
        List<ExecRow> rows = new ArrayList<ExecRow>(1000);
        WindowFunction rowNumber = new RowNumberFunction();

        rowNumber.setup(cf, "rowNumber", DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false));
        for (int i=0;i<1000;i++) {
            ValueRow valueRow = new ValueRow(4);
            valueRow.setColumn(1, dvf.getDataValue(i, null));
            valueRow.setColumn(2, dvf.getDataValue((double)i, null));
            valueRow.setColumn(3, dvf.getDataValue(""+i, null));
            rowNumber.accumulate(valueRow.getColumn(1), null);
            valueRow.setColumn(4, rowNumber.getResult().cloneValue(true));
            rows.add(valueRow);
        }
        for (ExecRow row: rows) {
            System.out.println(row);
        }

    }

}
