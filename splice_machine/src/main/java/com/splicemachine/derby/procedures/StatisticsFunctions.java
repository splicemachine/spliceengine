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

package com.splicemachine.derby.procedures;

import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.FakeColumnStatisticsImpl;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.catalog.Procedure;
import com.splicemachine.db.impl.sql.catalog.SystemColumnImpl;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
public class StatisticsFunctions {

    public static List<Procedure> getProcedures() throws StandardException {
        return Arrays.asList(
                /*
                 * Statistics functions
                 */
                Procedure.newBuilder().name("STATS_CARDINALITY")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .sqlControl(RoutineAliasInfo.NO_SQL)
                        .returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                        .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                        .arg("SOURCE", SystemColumnImpl
                                .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                .getType().getCatalogType())
                        .build(),
                Procedure.newBuilder().name("STATS_NULL_COUNT")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .sqlControl(RoutineAliasInfo.NO_SQL)
                        .returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                        .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                        .arg("SOURCE", SystemColumnImpl
                                .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                .getType().getCatalogType())
                        .build(),
                Procedure.newBuilder().name("STATS_NULL_FRACTION")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .sqlControl(RoutineAliasInfo.NO_SQL)
                        .returnType(DataTypeDescriptor.getCatalogType(Types.REAL))
                        .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                        .arg("SOURCE", SystemColumnImpl
                                .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                .getType().getCatalogType())
                        .build(),
                Procedure.newBuilder().name("STATS_QUANTILES")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .sqlControl(RoutineAliasInfo.NO_SQL)
                        .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                        .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                        .arg("SOURCE", SystemColumnImpl
                                .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                .getType().getCatalogType())
                        .build(),
                Procedure.newBuilder().name("STATS_FREQUENCIES")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .sqlControl(RoutineAliasInfo.NO_SQL)
                        .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                        .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                        .arg("SOURCE", SystemColumnImpl
                                .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                .getType().getCatalogType())
                        .build(),
                Procedure.newBuilder().name("STATS_THETA")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .sqlControl(RoutineAliasInfo.NO_SQL)
                        .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                        .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                        .arg("SOURCE", SystemColumnImpl
                                .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                .getType().getCatalogType())
                        .build(),

                Procedure.newBuilder().name("STATS_MIN")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .sqlControl(RoutineAliasInfo.NO_SQL)
                        .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                        .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                        .arg("SOURCE", SystemColumnImpl
                                .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                .getType().getCatalogType())
                        .build(),
                Procedure.newBuilder().name("STATS_MAX")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .sqlControl(RoutineAliasInfo.NO_SQL)
                        .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR))
                        .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                        .arg("SOURCE", SystemColumnImpl
                                .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                .getType().getCatalogType())
                        .build(),
                Procedure.newBuilder().name("STATS_COL_WIDTH")
                        .numOutputParams(0)
                        .numResultSets(0)
                        .sqlControl(RoutineAliasInfo.NO_SQL)
                        .returnType(DataTypeDescriptor.getCatalogType(Types.INTEGER))
                        .isDeterministic(true).ownerClass(StatisticsFunctions.class.getCanonicalName())
                        .arg("SOURCE", SystemColumnImpl
                                .getJavaColumn("DATA", ColumnStatisticsImpl.class.getCanonicalName(), false)
                                .getType().getCatalogType())
                        .build()
        );
    }

    public static int STATS_COL_WIDTH(ColumnStatisticsImpl itemStatistics) throws StandardException {
            if (itemStatistics == null) return 0;
            return itemStatistics.getColumnDescriptor().getLength(); // TODO JL
    }

    public static long STATS_CARDINALITY(ColumnStatisticsImpl itemStatistics){
        if(itemStatistics==null) return 0;
        return itemStatistics.cardinality();
    }

    public static long STATS_NULL_COUNT(ColumnStatisticsImpl itemStatistics){
        if(itemStatistics==null) return 0;
        return itemStatistics.nullCount();
    }

    public static float STATS_NULL_FRACTION(ColumnStatisticsImpl itemStatistics){
        if(itemStatistics==null) return 0;
        if (itemStatistics.notNullCount() + itemStatistics.nullCount() == 0) // Divide by null check
            return 0;
        return ((float) itemStatistics.nullCount()/ (float) (itemStatistics.notNullCount()+itemStatistics.nullCount()));
    }

    public static String STATS_MAX(ColumnStatisticsImpl itemStatistics) throws StandardException {
        if(itemStatistics==null) return null;
        DataValueDescriptor dvd = (DataValueDescriptor)itemStatistics.maxValue();
        if(dvd==null || dvd.isNull()) return null;
        return dvd.getString();
    }

    public static String STATS_MIN(ColumnStatisticsImpl itemStatistics) throws StandardException {
        if(itemStatistics==null) return null;
        DataValueDescriptor dvd = (DataValueDescriptor)itemStatistics.minValue();
        if(dvd==null || dvd.isNull()) return null;
        return dvd.getString();
    }

    public static String STATS_QUANTILES(ColumnStatisticsImpl itemStatistics) throws StandardException {
        if(itemStatistics==null || itemStatistics instanceof FakeColumnStatisticsImpl)
            return null;
        return itemStatistics.getQuantilesSketch().toString(true,true);
    }

    public static String STATS_FREQUENCIES(ColumnStatisticsImpl itemStatistics) throws StandardException {
        if(itemStatistics==null || itemStatistics instanceof FakeColumnStatisticsImpl)
            return null;
        return itemStatistics.getFrequenciesSketch().toString();
    }

    public static String STATS_THETA(ColumnStatisticsImpl itemStatistics) throws StandardException {
        if(itemStatistics==null || itemStatistics instanceof FakeColumnStatisticsImpl)
            return null;
        return itemStatistics.getThetaSketch().toString();
    }

}
