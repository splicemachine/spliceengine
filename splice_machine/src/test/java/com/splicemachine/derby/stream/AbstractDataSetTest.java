/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.BooleanDataValue;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.db.iapi.types.HBaseRowLocation;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.primitives.Bytes;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by jleach on 4/15/15.
 */
public abstract class AbstractDataSetTest extends BaseStreamTest implements Serializable{

    public AbstractDataSetTest() {

    }

    protected abstract DataSet<ExecRow> getTenRowsTwoDuplicateRecordsDataSet();

    /*
    @Test
    public void testDistinct() {
        DataSet<ExecRow> ds = getTenRowsTwoDuplicateRecordsDataSet();
        Assert.assertEquals("Duplicate Rows Not Filtered", 10, ds.distinct().collect().size());
    }
    */

    @Test
    public void testMap() throws StandardException {
        DataSet<ExecRow> ds = getTenRowsTwoDuplicateRecordsDataSet();
        DataSet<ExecRow> mapppedDataSet = ds.map(new MapFunction());
        Iterator < ExecRow > it = mapppedDataSet.toLocalIterator();
        while (it.hasNext())
            Assert.assertTrue("transform did not replace value",it.next().getColumn(1) instanceof BooleanDataValue);
    }

    @Test
    public void testLocalIterator() {
        int numElements = 0;
        DataSet<ExecRow> ds = getTenRowsTwoDuplicateRecordsDataSet();
        Iterator<ExecRow> it = ds.toLocalIterator();
        while (it.hasNext() && it.next() !=null)
            numElements++;
        Assert.assertEquals("iterator loses records",BaseStreamTest.tenRowsTwoDuplicateRecords.size(),numElements);
    }

    @Test
    public void testIndex() throws Exception {
        DataSet<ExecRow> ds = getTenRowsTwoDuplicateRecordsDataSet();
        PairDataSet<RowLocation,ExecRow> pairDataSet = ds.index(new IndexPairFunction());
        Iterator < ExecRow > it = pairDataSet.values(null).toLocalIterator();
        int i = 0;
        while (it.hasNext()) {
            i++;
            ExecRow execRow = it.next();
            Assert.assertEquals("Key not set",1,execRow.getColumn(1).getInt());
        }
        Assert.assertEquals("Number of rows incorrect",11,i);
    }

    public static class MapFunction extends SpliceFunction<SpliceOperation,ExecRow,ExecRow> {

        public MapFunction() {
            super();
        }

        @Override
        public ExecRow call(ExecRow execRow) throws Exception {
            ExecRow clone = execRow.getClone();
            clone.setColumn(1,TestingDataType.BOOLEAN.getDataValueDescriptor());
            return clone;
        }
    }

    public static class IndexPairFunction extends SplicePairFunction<SpliceOperation,ExecRow, RowLocation,ExecRow> {

        public IndexPairFunction() {

        }

        @Override
        public RowLocation genKey(ExecRow execRow) {
            try {
                ExecRow clone = execRow.getClone();
                DataValueDescriptor dvd = TestingDataType.INTEGER.getDataValueDescriptor();
                dvd.setValue(1);
                clone.setColumn(1, dvd);
                return new HBaseRowLocation(Bytes.toBytes(execRow.getColumn(1).getInt()));
            } catch (Exception e) {
                throw new RuntimeException("Error");
            }
        }

        @Override
        public ExecRow genValue(ExecRow execRow) {
            try {
                ExecRow clone = execRow.getClone();
                DataValueDescriptor dvd = TestingDataType.INTEGER.getDataValueDescriptor();
                dvd.setValue(1);
                clone.setColumn(1, dvd);
                return clone;
            } catch (Exception e) {
                throw new RuntimeException("Error");
            }
        }

        @Override
        public Tuple2<RowLocation, ExecRow> call(ExecRow execRow) throws Exception {
            return new Tuple2(genKey(execRow),genValue(execRow));
        }
    }

}
