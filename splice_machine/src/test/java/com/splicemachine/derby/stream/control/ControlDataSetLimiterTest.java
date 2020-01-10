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
 *
 */

package com.splicemachine.derby.stream.control;

import com.splicemachine.EngineDriver;
import com.splicemachine.SqlEnvironment;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.sql.conn.ControlExecutionLimiter;
import com.splicemachine.db.iapi.sql.conn.ControlExecutionLimiterImpl;
import com.splicemachine.db.iapi.sql.conn.ResubmitDistributedException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jleach on 4/15/15.
 */
public class ControlDataSetLimiterTest {
    public static List<ExecRow> hundredRowsTwoDuplicateRecords;
    public static List<Tuple2<ExecRow,ExecRow>> hundredRows;
    public static List<Tuple2<ExecRow,ExecRow>> evenRows;

    static {
        ClassSize.setDummyCatalog();
        hundredRowsTwoDuplicateRecords = new ArrayList<ExecRow>(100);
        hundredRows = new ArrayList<>();
        evenRows = new ArrayList<>();
        for (int i = 0; i< 100; i++) {
            hundredRowsTwoDuplicateRecords.add(getExecRow(i));
            hundredRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i % 2, 1), getExecRow(i, 100)));
            if (i%2==0)
                evenRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i % 2, 1), getExecRow(i, 100)));
            if (i==2)
                hundredRowsTwoDuplicateRecords.add(getExecRow(i));
        }
    }

    @BeforeClass
    public static void setup() {
        SIEnvironment ese = Mockito.mock(SIEnvironment.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(ese.configuration().getThreadPoolMaxSize()).thenReturn(30);
        SIDriver.loadDriver(ese);
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

    @Test(expected = ResubmitDistributedException.class)
    public void testSubtractByKeyLimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        PairDataSet<ExecRow, ExecRow> set2 = new ControlPairDataSet(evenRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(20);
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<ExecRow> it = set1.subtractByKey(set2, context).values(null).toLocalIterator();
        it.next();
    }

    @Test
    public void testSubtractByKeyUnlimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        PairDataSet<ExecRow, ExecRow> set2 = new ControlPairDataSet(evenRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(200);
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<ExecRow> it = set1.subtractByKey(set2, context).values(null).toLocalIterator();
        int i =0;
        while (it.hasNext()) {
            ExecRow row = it.next();
            if (row.getColumn(1).getInt()%2==0)
                Assert.assertTrue("Not Subtracted", false);
            else
                Assert.assertEquals("Join Issue", 1,row.getColumn(1).getInt()%2);
            i++;
        }
        Assert.assertEquals("Incorrect Number of Rows", 50, i);
    }


    @Test(expected = ResubmitDistributedException.class)
    public void testSortLimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(99);
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<ExecRow> it = set1.sortByKey(new Comparator<ExecRow>() {
            @Override
            public int compare(ExecRow o1, ExecRow o2) {
                return 0;
            }
        }, context).values(null).toLocalIterator();
        it.next();
    }

    @Test
    public void testSortUnlimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(101);
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<ExecRow> it = set1.sortByKey(new Comparator<ExecRow>() {
            @Override
            public int compare(ExecRow o1, ExecRow o2) {
                return 0;
            }
        }, context).values(null).toLocalIterator();
        it.next();
    }

    @Test(expected = ResubmitDistributedException.class)
    public void testGroupLimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(99);
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<Iterable<ExecRow>> it = set1.groupByKey(context).values(null).toLocalIterator();
        it.next();
    }

    @Test
    public void testGroupUnlimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(101);
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<Iterable<ExecRow>> it = set1.groupByKey(context).values(null).toLocalIterator();
        it.next();
    }

    @Test(expected = ResubmitDistributedException.class)
    public void testCogroupLimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        PairDataSet<ExecRow, ExecRow> set2 = new ControlPairDataSet(evenRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(149);
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<Tuple2<Iterable<ExecRow>, Iterable<ExecRow>>> it = set1.cogroup(set2, context).values(null).toLocalIterator();
        it.next();
    }

    @Test
    public void testCogroupUnlimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        PairDataSet<ExecRow, ExecRow> set2 = new ControlPairDataSet(evenRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(151);
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<Tuple2<Iterable<ExecRow>, Iterable<ExecRow>>> it = set1.cogroup(set2, context).values(null).toLocalIterator();
        it.next();
    }

    @Test(expected = ResubmitDistributedException.class)
    public void testHashjoinLimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        PairDataSet<ExecRow, ExecRow> set2 = new ControlPairDataSet(evenRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(49);   // only right side is materialized
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<Tuple2<ExecRow, ExecRow>> it = set1.hashJoin(set2, context).values(null).toLocalIterator();
        it.next();
    }

    @Test
    public void testHashjoinUnlimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        PairDataSet<ExecRow, ExecRow> set2 = new ControlPairDataSet(evenRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(51);   // only right side is materialized
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<Tuple2<ExecRow, ExecRow>> it = set1.hashJoin(set2, context).values(null).toLocalIterator();
        it.next();
    }

    @Test(expected = ResubmitDistributedException.class)
    public void testDistinctLimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        PairDataSet<ExecRow, ExecRow> set2 = new ControlPairDataSet(evenRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(99);
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<ExecRow> it = set1.values(null).union(set2.values(null), context).distinct(context).toLocalIterator();
        it.next();
    }

    @Test
    public void testDistinctUnlimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        PairDataSet<ExecRow, ExecRow> set2 = new ControlPairDataSet(evenRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(101);
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<ExecRow> it = set1.values(null).union(set2.values(null), context).distinct(context).toLocalIterator();
        it.next();
    }


    @Test(expected = ResubmitDistributedException.class)
    public void testSubtractLimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        PairDataSet<ExecRow, ExecRow> set2 = new ControlPairDataSet(evenRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(149);
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<ExecRow> it = set1.values(null).subtract(set2.values(null), context).toLocalIterator();
        it.next();
    }

    @Test
    public void testSubtractUnlimited() throws StandardException {
        PairDataSet<ExecRow, ExecRow> set1 = new ControlPairDataSet(hundredRows.iterator());
        PairDataSet<ExecRow, ExecRow> set2 = new ControlPairDataSet(evenRows.iterator());
        ControlExecutionLimiter limiter = new ControlExecutionLimiterImpl(151);
        OperationContext context = Mockito.mock(OperationContext.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter()).thenReturn(limiter);
        Iterator<ExecRow> it = set1.values(null).subtract(set2.values(null), context).toLocalIterator();
        it.next();
    }
}
