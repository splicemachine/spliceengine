package com.splicemachine.derby.stream;

import org.sparkproject.guava.common.collect.ArrayListMultimap;
import org.sparkproject.guava.common.collect.Multimap;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.test.TestingDataType;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jleach on 4/15/15.
 */
public class BaseStreamTest {
    public static List<ExecRow> tenRowsTwoDuplicateRecords;
    public static Multimap<ExecRow,ExecRow> tenRows;

    static {
        ClassSize.setDummyCatalog();
        tenRowsTwoDuplicateRecords = new ArrayList<ExecRow>(10);
        tenRows = ArrayListMultimap.create();
        for (int i = 0; i< 10; i++) {
            tenRowsTwoDuplicateRecords.add(getExecRow(i));
            tenRows.put(getExecRow(i%2,1),getExecRow(i,10));
            if (i==2)
                tenRowsTwoDuplicateRecords.add(getExecRow(i));
        }
    }
    public static ExecRow getExecRow(int value) {
        try {
            ValueRow vr = new ValueRow(2);
            DataValueDescriptor dvd = TestingDataType.VARCHAR.getDataValueDescriptor();
            dvd.setValue(""+value);
            DataValueDescriptor dvd2 = TestingDataType.VARCHAR.getDataValueDescriptor();
            dvd2.setValue(""+value);
            vr.setColumn(1, dvd);
            vr.setColumn(2, dvd2);
            return vr;
        } catch (StandardException se) {
            return null;
        }
    }

    public static ExecRow getExecRow(int value, int numberOfRecords) {
        try {
            ValueRow vr = new ValueRow(numberOfRecords);
            for (int i = 0; i<numberOfRecords;i++) {
                DataValueDescriptor dvd = TestingDataType.INTEGER.getDataValueDescriptor();
                dvd.setValue(value);
                vr.setColumn(i + 1, dvd);
            }
            return vr;
        } catch (StandardException se) {
            return null;
        }
    }
}
