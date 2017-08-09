/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
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
            DataValueDescriptor dvd = new SQLVarchar();
            dvd.setValue(""+value);
            DataValueDescriptor dvd2 = new SQLVarchar();
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
                DataValueDescriptor dvd = new SQLInteger();
                dvd.setValue(value);
                vr.setColumn(i + 1, dvd);
            }
            return vr;
        } catch (StandardException se) {
            return null;
        }
    }
}
