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

package com.splicemachine.derby.stream;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.experimental.categories.Category;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jleach on 4/15/15.
 */
@Category(ArchitectureIndependent.class)
public class BaseStreamTest {
    public static List<ExecRow> tenRowsTwoDuplicateRecords;
    public static List<Tuple2<ExecRow,ExecRow>> tenRows;
    public static List<Tuple2<ExecRow,ExecRow>> evenRows;

    public BaseStreamTest() {

    }

    static {
        ClassSize.setDummyCatalog();
        tenRowsTwoDuplicateRecords = new ArrayList<ExecRow>(10);
        tenRows = new ArrayList<>();
        evenRows = new ArrayList<>();
        for (int i = 0; i< 10; i++) {
            tenRowsTwoDuplicateRecords.add(getExecRow(i));
            tenRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i % 2, 1), getExecRow(i, 10)));
            if (i%2==0)
                evenRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i % 2, 1), getExecRow(i, 10)));
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
